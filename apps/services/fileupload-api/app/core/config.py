import os

class Settings:
    APP_NAME = os.getenv("APP_NAME", "Apex FileUpload API")

    # MinIO
    # Senha fraca pois é acesso interno (entre containers.)
    # as senhas mestras entao em .env
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
    S3_BUCKET = os.getenv("S3_BUCKET", "raw-uploads")
    S3_REGION = os.getenv("S3_REGION", "us-east-1")

    # SQL Server (ODBC string direta)
    UPLOAD_SQL_CONN_STR = os.getenv("UPLOAD_SQL_CONN_STR")
    UPLOAD_PG_CONN_STR = os.getenv("UPLOAD_PG_CONN_STR")

    # Airflow API Server (Airflow 3.x)
    AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-apiserver:8080/airflow")
    AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
    AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "Fantasy16")

settings = Settings()
