# app/api/router_universe.py

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request, Depends, Form, Path
from fastapi.responses import RedirectResponse
from mysql.connector import MySQLConnection

from ..dependencies import get_db_connection, templates

router = APIRouter()


# ---------- Helpers ----------


def _fetch_exchanges(conn: MySQLConnection) -> List[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id, code, name
        FROM exchange
        ORDER BY code
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _fetch_universe_list(conn: MySQLConnection) -> List[Dict[str, Any]]:
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
            u.is_active,
            e.code AS exchange_code,
            e.name AS exchange_name
        FROM universe u
        JOIN exchange e ON e.id = u.exchange_id
        ORDER BY e.code, u.symbol
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _fetch_universe_by_id(
    conn: MySQLConnection,
    universe_id: int,
) -> Optional[Dict[str, Any]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT
            u.id,
            u.symbol,
            u.exchange_id,
            u.yfinance_ticker,
            u.bse_code,
            u.bse_ticker,
            u.isin,
            u.is_active
        FROM universe u
        WHERE u.id = %s
        """,
        (universe_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    return row


def _insert_universe(
    conn: MySQLConnection,
    symbol: str,
    exchange_id: int,
    yfinance_ticker: str,
    bse_code: str,
    bse_ticker: str,
    isin: str,
    is_active: bool,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO universe (
            symbol,
            exchange_id,
            yfinance_ticker,
            bse_code,
            bse_ticker,
            isin,
            is_active
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            symbol,
            exchange_id,
            yfinance_ticker or None,
            bse_code or None,
            bse_ticker or None,
            isin or None,
            1 if is_active else 0,
        ),
    )
    new_id = cursor.lastrowid
    cursor.close()
    return new_id


def _update_universe(
    conn: MySQLConnection,
    universe_id: int,
    symbol: str,
    exchange_id: int,
    yfinance_ticker: str,
    bse_code: str,
    bse_ticker: str,
    isin: str,
    is_active: bool,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE universe
        SET symbol = %s,
            exchange_id = %s,
            yfinance_ticker = %s,
            bse_code = %s,
            bse_ticker = %s,
            isin = %s,
            is_active = %s
        WHERE id = %s
        """,
        (
            symbol,
            exchange_id,
            yfinance_ticker or None,
            bse_code or None,
            bse_ticker or None,
            isin or None,
            1 if is_active else 0,
            universe_id,
        ),
    )
    cursor.close()


def _set_universe_active_flag(
    conn: MySQLConnection,
    universe_id: int,
    is_active: bool,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE universe
        SET is_active = %s
        WHERE id = %s
        """,
        (1 if is_active else 0, universe_id),
    )
    cursor.close()


# ---------- Routes ----------


@router.get("/")
def list_universe(
    request: Request,
    conn: MySQLConnection = Depends(get_db_connection),
):
    rows = _fetch_universe_list(conn)
    conn.close()
    return templates.TemplateResponse(
        "universe/list.html",
        {
            "request": request,
            "rows": rows,
        },
    )


@router.get("/new")
def show_new_universe_form(
    request: Request,
    conn: MySQLConnection = Depends(get_db_connection),
):
    exchanges = _fetch_exchanges(conn)
    conn.close()
    return templates.TemplateResponse(
        "universe/edit.html",
        {
            "request": request,
            "mode": "new",
            "item": None,
            "exchanges": exchanges,
        },
    )


@router.post("/new")
def create_universe(
    request: Request,
    symbol: str = Form(...),
    exchange_id: int = Form(...),
    yfinance_ticker: str = Form(default=""),
    bse_code: str = Form(default=""),
    bse_ticker: str = Form(default=""),
    isin: str = Form(default=""),
    is_active: Optional[str] = Form(default="1"),
    conn: MySQLConnection = Depends(get_db_connection),
):
    active_flag = is_active == "1"

    _ = _insert_universe(
        conn=conn,
        symbol=symbol.strip(),
        exchange_id=exchange_id,
        yfinance_ticker=yfinance_ticker.strip(),
        bse_code=bse_code.strip(),
        bse_ticker=bse_ticker.strip(),
        isin=isin.strip(),
        is_active=active_flag,
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url="/universe/", status_code=303)


@router.get("/{universe_id}/edit")
def show_edit_universe_form(
    request: Request,
    universe_id: int = Path(...),
    conn: MySQLConnection = Depends(get_db_connection),
):
    item = _fetch_universe_by_id(conn, universe_id)
    exchanges = _fetch_exchanges(conn)
    conn.close()

    return templates.TemplateResponse(
        "universe/edit.html",
        {
            "request": request,
            "mode": "edit",
            "item": item,
            "exchanges": exchanges,
        },
    )


@router.post("/{universe_id}/edit")
def update_universe(
    request: Request,
    universe_id: int = Path(...),
    symbol: str = Form(...),
    exchange_id: int = Form(...),
    yfinance_ticker: str = Form(default=""),
    bse_code: str = Form(default=""),
    bse_ticker: str = Form(default=""),
    isin: str = Form(default=""),
    is_active: Optional[str] = Form(default="1"),
    conn: MySQLConnection = Depends(get_db_connection),
):
    active_flag = is_active == "1"

    _update_universe(
        conn=conn,
        universe_id=universe_id,
        symbol=symbol.strip(),
        exchange_id=exchange_id,
        yfinance_ticker=yfinance_ticker.strip(),
        bse_code=bse_code.strip(),
        bse_ticker=bse_ticker.strip(),
        isin=isin.strip(),
        is_active=active_flag,
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url="/universe/", status_code=303)


@router.post("/{universe_id}/toggle")
def toggle_universe_active(
    universe_id: int = Path(...),
    conn: MySQLConnection = Depends(get_db_connection),
):
    item = _fetch_universe_by_id(conn, universe_id)
    if item:
        new_flag = not bool(item["is_active"])
        _set_universe_active_flag(conn, universe_id, new_flag)
        conn.commit()
    conn.close()
    return RedirectResponse(url="/universe/", status_code=303)
