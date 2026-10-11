#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
tenant_key="${1:-}"
if [[ ! "$tenant_key" =~ ^[a-z0-9][a-z0-9-]{1,29}[a-z0-9]$ ]]; then
  printf 'Usage: %s <tenant-key>\n' "$0" >&2
  exit 2
fi

tenant_env="tenants/$tenant_key/.env"
if [[ ! -f "$tenant_env" ]]; then
  printf 'Tenant %s is not provisioned; run setup-secrets.sh first.\n' "$tenant_key" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$tenant_env"
set +a

crm_compose() {
  docker compose --env-file "$tenant_env" -p "pulse-crm-$tenant_key" -f docker-compose.yml "$@"
}

crm_compose build odoo
crm_compose up -d --wait crm-db
database_exists="$(crm_compose exec -T crm-db psql -U "$CRM_DB_USER" -d postgres -Atqc \
  "SELECT 1 FROM pg_database WHERE datname = '$CRM_DB_NAME'")"
if [[ "$database_exists" == "1" ]]; then
  printf 'Database for tenant %s already exists; refusing to reset its admin password.\n' "$tenant_key" >&2
  exit 1
fi

crm_compose run --rm --no-deps odoo \
  odoo -c /etc/odoo/odoo.conf -d "$CRM_DB_NAME" \
  -i crm,auth_oidc,pulse_crm_sso --without-demo=all --stop-after-init --logfile=-

crm_compose run --rm --no-deps odoo \
  odoo shell -c /etc/odoo/odoo.conf -d "$CRM_DB_NAME" --no-http <<'PY'
import os

env.ref("base.user_admin").write(
    {"password": os.environ["CRM_INITIAL_ADMIN_PASSWORD"]}
)
PY
