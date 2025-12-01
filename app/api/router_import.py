# app/api/router_import.py

from datetime import date, datetime, timedelta
from io import TextIOWrapper
import csv
from typing import List, Dict, Any

from fastapi import APIRouter, Request, Depends, Form, UploadFile, File
from fastapi.responses import RedirectResponse
from mysql.connector import MySQLConnection
import yfinance as yf

from ..dependencies import get_db_connection, templates

router = APIRouter()


# ---------- Helpers (common DB) ----------


def _fetch_universe_yfinance(conn: MySQLConnection) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT u.id, u.symbol, u.yfinance_ticker, e.code AS exchange_code
        FROM universe u
        JOIN exchange e ON e.id = u.exchange_id
        WHERE u.is_active = 1
          AND u.yfinance_ticker IS NOT NULL
          AND u.yfinance_ticker <> ''
        ORDER BY e.code, u.symbol
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _insert_or_update_price(
    conn: MySQLConnection,
    universe_id: int,
    trade_date: date,
    open_p: float,
    high_p: float,
    low_p: float,
    close_p: float,
    volume: int,
    source: str,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO price_history (
            universe_id, trade_date, `open`, `high`, `low`, `close`, volume, source
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            `open` = VALUES(`open`),
            `high` = VALUES(`high`),
            `low`  = VALUES(`low`),
            `close`= VALUES(`close`),
            volume = VALUES(volume),
            source = VALUES(source)
        """,
        (universe_id, trade_date, open_p, high_p, low_p, close_p, volume, source),
    )
    cursor.close()


# ---------- YFINANCE IMPORT ----------


@router.get("/yfinance")
def show_yfinance_form(request: Request):
    """
    Show form to import OHLCV data via yfinance for all active universe
    with yfinance_ticker set.
    """
    return templates.TemplateResponse(
        "import/yfinance_form.html",
        {"request": request},
    )


@router.post("/yfinance/run")
def run_yfinance_import(
    request: Request,
    start_date: str = Form(...),
    end_date: str = Form(...),
    conn: MySQLConnection = Depends(get_db_connection),
):
    """
    Fetch data from yfinance for all suitable universe stocks
    between start_date and end_date (inclusive).
    """
    msg_list: List[str] = []

    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError:
        return templates.TemplateResponse(
            "import/import_log.html",
            {
                "request": request,
                "messages": ["Invalid date format. Use YYYY-MM-DD."],
            },
        )

    # yfinance end is exclusive; add 1 day
    yf_end = end + timedelta(days=1)

    universe_rows = _fetch_universe_yfinance(conn)

    for row in universe_rows:
        uid = row["id"]
        ticker = row["yfinance_ticker"]

        try:
            df = yf.download(
                ticker,
                start=start,
                end=yf_end,
                progress=False,
                auto_adjust=False,
            )
        except Exception as exc:  # keep simple
            msg_list.append(f"{ticker}: ERROR fetching data: {exc}")
            continue

        if df is None or df.empty:
            msg_list.append(f"{ticker}: no data returned.")
            continue

        count = 0
        for idx, record in df.iterrows():
            try:
                # idx is pandas.Timestamp
                d = idx.date()
                open_p = float(record["Open"])
                high_p = float(record["High"])
                low_p = float(record["Low"])
                close_p = float(record["Close"])
                vol = int(record["Volume"]) if not record["Volume"] != record["Volume"] else 0  # NaN check

                _insert_or_update_price(
                    conn,
                    universe_id=uid,
                    trade_date=d,
                    open_p=open_p,
                    high_p=high_p,
                    low_p=low_p,
                    close_p=close_p,
                    volume=vol,
                    source="YFINANCE",
                )
                count += 1
            except Exception as exc:
                msg_list.append(f"{ticker}: error on row {idx}: {exc}")

        msg_list.append(f"{ticker}: imported/updated {count} rows.")

    conn.commit()
    conn.close()

    return templates.TemplateResponse(
        "import/import_log.html",
        {
            "request": request,
            "messages": msg_list,
        },
    )


# ---------- BSE BHAV IMPORT ----------


def _create_bhav_upload_row(
    conn: MySQLConnection,
    file_name: str,
    trade_date: date,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO bhav_upload (file_name, trade_date, status)
        VALUES (%s, %s, 'PENDING')
        """,
        (file_name, trade_date),
    )
    upload_id = cursor.lastrowid
    cursor.close()
    return upload_id


def _update_bhav_upload_row(
    conn: MySQLConnection,
    upload_id: int,
    status: str,
    records_total: int,
    records_loaded: int,
    error_message: str | None = None,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE bhav_upload
        SET status = %s,
            records_total = %s,
            records_loaded = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, records_total, records_loaded, error_message, upload_id),
    )
    cursor.close()


def _fetch_universe_bse_map(conn: MySQLConnection) -> Dict[str, int]:
    """
    Return mapping FinInstrmId (bse_code) -> universe_id for active universe.
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id, bse_code
        FROM universe
        WHERE is_active = 1
          AND bse_code IS NOT NULL
          AND bse_code <> ''
        """
    )
    rows = cursor.fetchall()
    cursor.close()

    mapping: Dict[str, int] = {}
    for r in rows:
        if r["bse_code"]:
            mapping[str(r["bse_code"]).strip()] = r["id"]
    return mapping


@router.get("/bhav")
def show_bhav_form(request: Request):
    """
    Show form to upload BSE bhav copy file.
    """
    return templates.TemplateResponse(
        "import/bhav_upload.html",
        {"request": request},
    )


@router.post("/bhav/upload")
async def upload_bhav_file(
    request: Request,
    trade_date: str = Form(...),
    file: UploadFile = File(...),
    conn: MySQLConnection = Depends(get_db_connection),
):
    """
    Upload and process a BSE bhav copy CSV.
    Expected columns (from your sample):
      - TradDt
      - FinInstrmId
      - TckrSymb
      - ISIN
      - OpnPric
      - HghPric
      - LwPric
      - ClsPric
      - TtlTradgVol
    """
    messages: List[str] = []

    try:
        tdate = date.fromisoformat(trade_date)
    except ValueError:
        return templates.TemplateResponse(
            "import/import_log.html",
            {
                "request": request,
                "messages": ["Invalid trade date. Use YYYY-MM-DD."],
            },
        )

    upload_id = _create_bhav_upload_row(conn, file.filename, tdate)

    total = 0
    loaded = 0
    error_msg: str | None = None

    try:
        # universe mapping by bse_code (FinInstrmId)
        bse_map = _fetch_universe_bse_map(conn)

        # wrap file stream for csv
        text_stream = TextIOWrapper(file.file, encoding="utf-8", newline="")
        reader = csv.DictReader(text_stream)

        for row in reader:
            total += 1

            try:
                fin_id = str(row.get("FinInstrmId", "")).strip()
                if not fin_id:
                    continue

                universe_id = bse_map.get(fin_id)
                if not universe_id:
                    continue  # stock not in universe

                # parse prices
                opn = float(row.get("OpnPric", "0") or 0)
                hig = float(row.get("HghPric", "0") or 0)
                low = float(row.get("LwPric", "0") or 0)
                cls = float(row.get("ClsPric", "0") or 0)
                vol = int(float(row.get("TtlTradgVol", "0") or 0))

                _insert_or_update_price(
                    conn,
                    universe_id=universe_id,
                    trade_date=tdate,
                    open_p=opn,
                    high_p=hig,
                    low_p=low,
                    close_p=cls,
                    volume=vol,
                    source="BSE_BHAV",
                )
                loaded += 1
            except Exception as row_exc:
                # do not fail entire file; just log
                messages.append(f"Row error (FinInstrmId={row.get('FinInstrmId')}): {row_exc}")

        conn.commit()
        status = "SUCCESS"
    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        conn.rollback()

    _update_bhav_upload_row(conn, upload_id, status, total, loaded, error_msg)
    conn.close()

    messages.insert(0, f"Bhav upload status: {status}, total rows={total}, loaded={loaded}")
    if error_msg:
        messages.append(f"Error: {error_msg}")

    return templates.TemplateResponse(
        "import/import_log.html",
        {
            "request": request,
            "messages": messages,
        },
    )
