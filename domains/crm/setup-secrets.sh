#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
if [[ $# -ne 2 ]]; then
  printf 'Usage: %s <tenant-key> <odoo-loopback-port>\n' "$0" >&2
  exit 2
fi

repo_root="$(cd "$PWD/../.." && pwd)"
python3 "$repo_root/infra/keycloak/provision_crm_tenant.py" "$1" --odoo-port "$2"
