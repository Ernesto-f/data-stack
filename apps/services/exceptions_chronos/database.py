import pyodbc
import os
from collections.abc import Generator
from typing import Optional

class Database:
    def __init__(self):
        self.conn: Optional[pyodbc.Connection] = None

    def connect(self):
        self.conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('MSSQL_HOST')};"
            f"DATABASE={os.getenv('MSSQL_DB', 'db_mcExperienceSuite_mirror')};"
            f"UID={os.getenv('MSSQL_USER')};"
            f"PWD={os.getenv('MSSQL_PASS')};"
            f"TrustServerCertificate=yes;"
        )
        self.conn.autocommit = False

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def cursor(self):
        conn = self.conn
        if conn is None:
            raise RuntimeError("Database connection is not initialized")
        return conn.cursor()

    def commit(self):
        conn = self.conn
        if conn is None:
            raise RuntimeError("Database connection is not initialized")
        conn.commit()

    def rollback(self):
        conn = self.conn
        if conn is None:
            raise RuntimeError("Database connection is not initialized")
        conn.rollback()


def get_db() -> Generator[Database, None, None]:
    db = Database()
    db.connect()
    try:
        yield db
    finally:
        db.disconnect()
