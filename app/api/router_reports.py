# app/api/router_reports.py

from typing import Any, Dict, List

from fastapi import APIRouter, Request, Depends, Path
from mysql.connector import MySQLConnection

from ..dependencies import get_db_connection, templates

router = APIRouter()


# ---------- Helpers ----------


def _fetch_latest_run(conn: MySQLConnection) -> Dict[str, Any] | None:
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
    cursor.close()
    return run


def _fetch_summary_by_exchange_and_trend(
    conn: MySQLConnection,
    run_id: int,
) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT
            e.code AS exchange_code,
            e.name AS exchange_name,
            fr.trend_flag,
            COUNT(*) AS cnt
        FROM forecast_result fr
        JOIN universe u ON u.id = fr.universe_id
        JOIN exchange e ON e.id = u.exchange_id
        WHERE fr.forecast_run_id = %s
        GROUP BY e.code, e.name, fr.trend_flag
        ORDER BY e.code, fr.trend_flag
        """,
        (run_id,),
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _fetch_universe_info(
    conn: MySQLConnection,
    universe_id: int,
) -> Dict[str, Any] | None:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT
            u.id,
            u.symbol,
            u.yfinance_ticker,
            u.bse_code,
            u.bse_ticker,
            u.isin,
            e.code AS exchange_code,
            e.name AS exchange_name
        FROM universe u
        JOIN exchange e ON e.id = u.exchange_id
        WHERE u.id = %s
        """,
        (universe_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    return row


def _fetch_price_history(
    conn: MySQLConnection,
    universe_id: int,
    limit_rows: int = 120,
) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT
            trade_date,
            `open`,
            `high`,
            `low`,
            `close`,
            volume,
            source
        FROM price_history
        WHERE universe_id = %s
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (universe_id, limit_rows),
    )
    rows = cursor.fetchall()
    cursor.close()
    rows.reverse()
    return rows


def _fetch_recent_forecasts(
    conn: MySQLConnection,
    universe_id: int,
    limit_rows: int = 20,
) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT
            fr.id,
            fr.as_of_date,
            fr.next_date,
            fr.last_close,
            fr.forecast_return,
            fr.forecast_price,
            fr.lower_price,
            fr.upper_price,
            fr.trend_flag,
            fr.created_at,
            r.run_time,
            r.description AS run_description
        FROM forecast_result fr
        JOIN forecast_run r ON r.id = fr.forecast_run_id
        WHERE fr.universe_id = %s
        ORDER BY fr.as_of_date DESC, fr.id DESC
        LIMIT %s
        """,
        (universe_id, limit_rows),
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


# ---------- Routes ----------


@router.get("/summary")
def show_summary(
    request: Request,
    conn: MySQLConnection = Depends(get_db_connection),
):
    """
    Summary of latest forecast run: count of UP/DOWN/FLAT per exchange.
    """
    run = _fetch_latest_run(conn)
    if not run:
        conn.close()
        return templates.TemplateResponse(
            "reports/summary.html",
            {
                "request": request,
                "run": None,
                "rows": [],
            },
        )

    rows = _fetch_summary_by_exchange_and_trend(conn, run["id"])
    conn.close()

    return templates.TemplateResponse(
        "reports/summary.html",
        {
            "request": request,
            "run": run,
            "rows": rows,
        },
    )


@router.get("/stock/{universe_id}")
def show_stock_report(
    request: Request,
    universe_id: int = Path(...),
    conn: MySQLConnection = Depends(get_db_connection),
):
    """
    Per-stock report:
      - basic universe info
      - recent price history
      - recent forecast results
    """
    universe = _fetch_universe_info(conn, universe_id)
    if not universe:
        conn.close()
        return templates.TemplateResponse(
            "reports/stock_history.html",
            {
                "request": request,
                "universe": None,
                "prices": [],
                "forecasts": [],
            },
        )

    prices = _fetch_price_history(conn, universe_id)
    forecasts = _fetch_recent_forecasts(conn, universe_id)
    conn.close()

    return templates.TemplateResponse(
        "reports/stock_history.html",
        {
            "request": request,
            "universe": universe,
            "prices": prices,
            "forecasts": forecasts,
        },
    )
