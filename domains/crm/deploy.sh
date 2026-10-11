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

docker compose --env-file "$tenant_env" -p "pulse-crm-$tenant_key" \
  -f docker-compose.yml build odoo
docker compose --env-file "$tenant_env" -p "pulse-crm-$tenant_key" \
  -f docker-compose.yml up -d --wait
docker compose --env-file "$tenant_env" -p "pulse-crm-$tenant_key" \
  -f docker-compose.yml ps
