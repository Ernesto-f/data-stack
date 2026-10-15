import os


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável de ambiente obrigatória não configurada: {name}"
        )

    return value


DATABASE_URL_GSUNNY = get_required_env(
    "DATABASE_URL_GSUNNY"
)

DATABASE_ECHO_GSUNNY = (
    os.getenv("DATABASE_ECHO_GSUNNY", "false")
    .strip()
    .lower()
    in {"true", "1", "yes", "on"}
)