#########################################
"""
FUNÇÃO COMMON PARA PEGAR O WORKDAY OLHANDO 
PARA O BANCO DETERMINADO ABAIXO EM "SQL_CONNECTION"
"""
#########################################




import pyodbc
from commom.connections import _odbc_connect_str, get_src_conn

_ALLOWED_DATEPARTS = {
    "DAY", "DD",
    "WEEK", "WK",
    "MONTH", "MM",
    "YEAR", "YY",
    "HOUR", "HH",
    "MINUTE", "MI",
    "SECOND", "SS",
}

def fn_GetWorkDay(
    period_name: str, 
    period_value: int,
    workday_role: str | None = "workday"):
    if period_name is None:
        raise ValueError("period_name não pode ser None")
    if period_value is None:
        raise ValueError("period_value não pode ser None")

    datepart = period_name.strip().upper()
    if datepart not in _ALLOWED_DATEPARTS:
        raise ValueError(f"period_name inválido para DATEADD: {period_name!r}")

    SQL_CONNECTION = "sql_udi_chronos"
    if workday_role == "workday":
        sql = f"""
        SELECT CAST(DATEADD({datepart}, ?, db_mcIOTGatewayService.dbo.fn_GetWorkDay(GETDATE())) AS date) AS WorkDay
        """
    else:
        sql = f"""
        SELECT CAST(DATEADD({datepart}, ?, GETDATE()) AS date) AS WorkDay
        """        

    with get_src_conn(SQL_CONNECTION) as conn:
        cur = conn.cursor()
        cur.execute(sql, (int(period_value),))
        row = cur.fetchone()
        return row[0] if row else None
