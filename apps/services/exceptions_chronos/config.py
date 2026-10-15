import os

DB         = os.getenv("MSSQL_DB",        "db_mcExperienceSuite_mirror")
DEPARTMENT = os.getenv("APP_DEPARTMENT",  "Engenharia")
FACILITY   = os.getenv("APP_FACILITY",    "Expedição")
