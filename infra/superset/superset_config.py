# Habilita proxy e usa subpath /superset atrás do Nginx
ENABLE_PROXY_FIX = True

# Se acessar via subpath:
URL_PREFIX = '/superset'
# Ajuste a URL base (opcional; útil para links absolutos)
# WEBSERVER_BASEURL = 'https://SEU_DOMINIO/superset'

# Segurança básica
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False  # True se usar HTTPS
WTF_CSRF_ENABLED = True

# Performance/UX
ROW_LIMIT = 50000
SIP_15_ENABLED = True