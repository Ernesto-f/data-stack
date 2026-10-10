#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p config

if [[ ! -f .env ]]; then
  umask 077
  db_password="$(openssl rand -hex 32)"
  master_password="$(openssl rand -hex 32)"
  admin_password="$(openssl rand -hex 32)"
  printf '%s\n' \
    'CRM_DB_USER=pulse_crm' \
    "CRM_DB_PASSWORD=$db_password" \
    "CRM_MASTER_PASSWORD=$master_password" \
    "CRM_INITIAL_ADMIN_PASSWORD=$admin_password" > .env
  chmod 600 .env
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

if [[ ! -f config/odoo.conf ]]; then
  umask 077
  sed \
    -e "s/REPLACED_BY_setup-secrets.sh/$CRM_MASTER_PASSWORD/1" \
    -e "s/REPLACED_BY_setup-secrets.sh/$CRM_DB_USER/1" \
    -e "s/REPLACED_BY_setup-secrets.sh/$CRM_DB_PASSWORD/1" \
    config/odoo.conf.example > config/odoo.conf
  chmod 600 config/odoo.conf
fi

# Odoo's official image runs as UID 100, GID 101.
sudo chown 100:101 config/odoo.conf
sudo chmod 400 config/odoo.conf

printf '%s\n' 'CRM demo credentials prepared locally (values not displayed).'
