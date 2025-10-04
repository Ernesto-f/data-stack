import os

# Rodando atrás de proxy reverso
ENABLE_PROXY_FIX = True

# Superset servido em subpath pelo Nginx
# (manter as duas para evitar problemas em versões/configs diferentes)
APPLICATION_ROOT = "/superset"
URL_PREFIX = "/superset"

# Esquema preferido; mude para "https" quando habilitar TLS
PREFERRED_URL_SCHEME = "http"

# Segurança de cookies
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False  # mude para True quando usar HTTPS

# CSRF ON
WTF_CSRF_ENABLED = True

# Limites e performance
ROW_LIMIT = 50000

# Deixe a URI do metastore vir do docker-compose (env)
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")