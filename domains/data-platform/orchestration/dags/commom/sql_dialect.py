from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SqlDialect:
    name: str
    limit_style: str      # "top" | "limit"
    nolock_hint: str      # "" | "WITH (NOLOCK)"
    null_func: str        # "ISNULL" | "COALESCE" | "IFNULL"
    ql: str               # quote left
    qr: str               # quote right

    def select_limit_prefix(self, limit: int | None) -> str:
        if not limit:
            return ""
        if self.limit_style == "top":
            return f"TOP ({limit})"
        return ""

    def select_limit_suffix(self, limit: int | None) -> str:
        if not limit:
            return ""
        if self.limit_style == "limit":
            return f"LIMIT {limit}"
        return ""

    def quote_ident(self, ident: str | None) -> str:
        ident = (ident or "").strip()

        if not ident:
            return ident

        # Se já veio qualificado, com alias ou com função, não mexe.
        # Ex.: t.Cod_Record, dbo.tabela, public.tabela, COUNT(*)
        if "." in ident or "(" in ident or ")" in ident:
            return ident

        # Se já veio quotado, mantém.
        if (
            ident.startswith("[")
            or ident.startswith('"')
            or ident.startswith("`")
        ):
            return ident

        return f"{self.ql}{ident}{self.qr}"

    def quote_qualified_ident(self, ident: str | None) -> str:
        """
        Quota identificadores qualificados.

        Ex:
        public.tabela -> "public"."tabela"
        dbo.tabela    -> [dbo].[tabela]
        """

        ident = (ident or "").strip()

        if not ident:
            return ident

        # Se tiver alias, função ou expressão, não mexe.
        if " " in ident or "(" in ident or ")" in ident:
            return ident

        return ".".join(
            self.quote_ident(part)
            for part in ident.split(".")
        )


_DIALECTS = {
    "mssql": SqlDialect(
        name="mssql",
        limit_style="top",
        nolock_hint="WITH (NOLOCK)",
        null_func="ISNULL",
        ql="[",
        qr="]",
    ),
    "postgres": SqlDialect(
        name="postgres",
        limit_style="limit",
        nolock_hint="",
        null_func="COALESCE",
        ql='"',
        qr='"',
    ),
    "mysql": SqlDialect(
        name="mysql",
        limit_style="limit",
        nolock_hint="",
        null_func="IFNULL",
        ql="`",
        qr="`",
    ),
}


def detect_dialect(conn_sig: str) -> str:
    s = (conn_sig or "").lower().strip()

    if s in ("mysql", "mariadb", "mysql+mysqlconnector", "mysql+pymysql"):
        return "mysql"

    if s in ("postgres", "postgresql", "postgresql+psycopg2", "postgresql+psycopg"):
        return "postgres"

    if s in ("mssql", "microsoft mssql", "ms_sql", "mssql+pyodbc", "sqlserver"):
        return "mssql"

    if (
        "sql server" in s
        or "odbc driver 18 for sql server" in s
        or "odbc driver 17 for sql server" in s
        or "driver={sql server" in s
    ):
        return "mssql"

    if "postgresql" in s or "postgres" in s:
        return "postgres"

    if "mariadb" in s or "mysql" in s:
        return "mysql"

    raise ValueError(
        f"Dialeto não suportado/indetectável a partir de '{conn_sig}'."
    )


def get_dialect(conn_sig: str) -> SqlDialect:
    name = detect_dialect(conn_sig)
    return _DIALECTS[name]