# dependencies.py

from fastapi.templating import Jinja2Templates
from mysql.connector import connect, MySQLConnection


# ---------- Jinja2 Templates ----------
templates = Jinja2Templates(directory="app/templates")


# ---------- MySQL Connection ----------
def get_db_connection() -> MySQLConnection:
    """
    Very simple MySQL connection creator.
    No pooling, no ORM, no pydantic.
    Adjust credentials as needed.
    """
    conn = connect(
        host="localhost",
        user="root",
        password="your_password_here",
        database="stock_forecast",
        autocommit=False,
    )
    return conn
