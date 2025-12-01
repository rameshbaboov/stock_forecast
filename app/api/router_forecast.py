# app/api/router_forecast.py

from datetime import date, datetime, timedelta
from typing import List, Dict, Any

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import RedirectResponse
from mysql.connector import MySQLConnection

from ..dependencies import get_db_connection, templates

router = APIRouter()


# ---------- Helpers ----------


def _fetch_active_universe(conn: MySQLConnection) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT u.id, u.symbol, e.code AS exchange_code
        FROM universe u
        JOIN exchange e ON e.id = u.exchange_id
        WHERE u.is_active = 1
        ORDER BY e.code, u.symbol
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _fetch_recent_prices(
    conn: MySQLConnection,
    universe_id: int,
    limit_days: int = 60,
) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT trade_date, close, volume
        FROM price_history
        WHERE universe_id = %s
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (universe_id, limit_days),
    )
    rows = cursor.fetchall()
    cursor.close()
    # reverse to ascending by date for easier calculations
    rows.reverse()
    return rows


def _compute_forecast_from_prices(
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Implement simple non-ML forecast:
    - short MA (5), long MA (20)
    - drift (10)
    - volatility (20)
    """
    if len(rows) < 5:
        # not enough data, flat forecast
        last = rows[-1]
        last_close = float(last["close"])
        as_of = last["trade_date"]
        next_d = as_of + timedelta(days=1)
        return {
            "as_of_date": as_of,
            "next_date": next_d,
            "last_close": last_close,
            "forecast_return": 0.0,
            "forecast_price": last_close,
            "lower_price": last_close,
            "upper_price": last_close,
            "trend_flag": "FLAT",
            "details": {},
        }

    closes = [float(r["close"]) for r in rows]
    dates = [r["trade_date"] for r in rows]

    # daily returns
    returns: List[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        if prev != 0:
            returns.append((cur - prev) / prev)
        else:
            returns.append(0.0)

    last_close = closes[-1]
    as_of_date = dates[-1]
    next_date = as_of_date + timedelta(days=1)

    # windows (clamp sizes)
    short_n = min(5, len(closes))
    long_n = min(20, len(closes))
    drift_n = min(10, len(returns))
    vol_n = min(20, len(returns))

    ma_short = sum(closes[-short_n:]) / short_n
    ma_long = sum(closes[-long_n:]) / long_n
    drift = sum(returns[-drift_n:]) / drift_n if drift_n > 0 else 0.0

    # volatility (std dev)
    if vol_n > 1:
        last_rets = returns[-vol_n:]
        mean_r = sum(last_rets) / vol_n
        var = sum((r - mean_r) ** 2 for r in last_rets) / (vol_n - 1)
        vol = var ** 0.5
    else:
        vol = 0.0

    trend = ma_short - ma_long

    # rules
    if trend > 0 and drift > 0:
        forecast_return = min(drift * 1.2, 0.01)
        trend_flag = "UP"
    elif trend < 0 and drift < 0:
        forecast_return = -min(abs(drift) * 1.2, 0.01)
        trend_flag = "DOWN"
    else:
        forecast_return = 0.0
        trend_flag = "FLAT"

    forecast_price = last_close * (1.0 + forecast_return)
    lower_price = forecast_price * (1.0 - vol) if vol > 0 else forecast_price
    upper_price = forecast_price * (1.0 + vol) if vol > 0 else forecast_price

    return {
        "as_of_date": as_of_date,
        "next_date": next_date,
        "last_close": last_close,
        "forecast_return": forecast_return,
        "forecast_price": forecast_price,
        "lower_price": lower_price,
        "upper_price": upper_price,
        "trend_flag": trend_flag,
        "details": {
            "ma_short": ma_short,
            "ma_long": ma_long,
            "drift": drift,
            "volatility": vol,
        },
    }


def _create_forecast_run(
    conn: MySQLConnection,
    description: str | None,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO forecast_run (run_time, description, status)
        VALUES (%s, %s, %s)
        """,
        (datetime.utcnow(), description, "SUCCESS"),
    )
    run_id = cursor.lastrowid
    cursor.close()
    return run_id


def _update_forecast_run_status(
    conn: MySQLConnection,
    run_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE forecast_run
        SET status = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, error_message, run_id),
    )
    cursor.close()


def _insert_forecast_result(
    conn: MySQLConnection,
    run_id: int,
    universe_id: int,
    forecast: Dict[str, Any],
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO forecast_result (
            forecast_run_id,
            universe_id,
            as_of_date,
            next_date,
            last_close,
            forecast_return,
            forecast_price,
            lower_price,
            upper_price,
            trend_flag,
            details_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            run_id,
            universe_id,
            forecast["as_of_date"],
            forecast["next_date"],
            forecast["last_close"],
            forecast["forecast_return"],
            forecast["forecast_price"],
            forecast["lower_price"],
            forecast["upper_price"],
            forecast["trend_flag"],
            None,  # keep simple for now; can dump JSON later
        ),
    )
    cursor.close()


def _fetch_latest_run_results(conn: MySQLConnection) -> Dict[str, Any]:
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT id, run_time, description, status, error_message
        FROM forecast_run
        ORDER BY run_time DESC
        LIMIT 1
        """
    )
    run = cursor.fetchone()
    if not run:
        cursor.close()
        return {"run": None, "results": []}

    cursor.execute(
        """
        SELECT
            fr.id,
            fr.universe_id,
            u.symbol,
            e.code AS exchange_code,
            fr.as_of_date,
            fr.next_date,
            fr.last_close,
            fr.forecast_return,
            fr.forecast_price,
            fr.lower_price,
            fr.upper_price,
            fr.trend_flag
        FROM forecast_result fr
        JOIN universe u ON u.id = fr.universe_id
        JOIN exchange e ON e.id = u.exchange_id
        WHERE fr.forecast_run_id = %s
        ORDER BY e.code, u.symbol
        """,
        (run["id"],),
    )
    results = cursor.fetchall()
    cursor.close()
    return {"run": run, "results": results}


# ---------- Routes ----------


@router.get("/run")
def show_run_form(request: Request):
    """
    Simple page to trigger forecast generation for all active universe stocks.
    """
    return templates.TemplateResponse(
        "forecast/run.html",
        {
            "request": request,
        },
    )


@router.post("/run")
def run_forecast(
    request: Request,
    description: str = Form(default=""),
    conn: MySQLConnection = Depends(get_db_connection),
):
    """
    Run forecast for all active universe stocks and redirect to list page.
    """
    run_id: int | None = None
    try:
        run_id = _create_forecast_run(conn, description or None)

        universe_rows = _fetch_active_universe(conn)

        for row in universe_rows:
            prices = _fetch_recent_prices(conn, row["id"])
            if not prices:
                # no data, skip
                continue

            forecast = _compute_forecast_from_prices(prices)
            _insert_forecast_result(conn, run_id, row["id"], forecast)

        conn.commit()
    except Exception as exc:  # keep it simple
        if run_id is not None:
            _update_forecast_run_status(conn, run_id, "FAILED", str(exc))
            conn.commit()
        raise
    finally:
        conn.close()

    return RedirectResponse(url="/forecast/latest", status_code=303)


@router.get("/latest")
def show_latest_forecast(
    request: Request,
    conn: MySQLConnection = Depends(get_db_connection),
):
    """
    Show results of the latest forecast run.
    """
    data = _fetch_latest_run_results(conn)
    conn.close()

    return templates.TemplateResponse(
        "forecast/list.html",
        {
            "request": request,
            "run": data["run"],
            "results": data["results"],
        },
    )
