#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
[[ -f .env && -f config/odoo.conf ]] || {
  printf '%s\n' 'Run ./setup-secrets.sh first.' >&2
  exit 1
}

set -a
# shellcheck disable=SC1091
source .env
set +a

docker compose up -d --wait crm-db

if docker compose exec -T crm-db psql -U "$CRM_DB_USER" -d postgres -Atqc \
  "SELECT 1 FROM pg_database WHERE datname = 'pulse_crm_demo'" | grep -q 1; then
  printf '%s\n' 'pulse_crm_demo already exists; refusing to reset its admin password.' >&2
  exit 1
fi

docker compose run --rm --no-deps odoo \
  odoo -c /etc/odoo/odoo.conf -d pulse_crm_demo \
  -i crm,auth_oidc,pulse_crm_sso --without-demo=all --stop-after-init --logfile=-

docker compose run --rm --no-deps odoo \
  odoo shell -c /etc/odoo/odoo.conf -d pulse_crm_demo --no-http <<'PY'
import os

env.ref("base.user_admin").write(
    {"password": os.environ["CRM_INITIAL_ADMIN_PASSWORD"]}
)
PY
