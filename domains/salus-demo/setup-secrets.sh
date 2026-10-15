#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p config

if [[ ! -f .env ]]; then
  umask 077
  erp_db_password="$(openssl rand -hex 32)"
  erp_admin_password="$(openssl rand -hex 32)"
  lims_db_password="$(openssl rand -hex 32)"
  lims_admin_password="$(openssl rand -hex 32)"
  printf '%s\n' \
    'ERP_POSTGRES_USER=salus_erp_demo' \
    "ERP_POSTGRES_PASSWORD=$erp_db_password" \
    "ERP_ADMIN_PASSWORD=$erp_admin_password" \
    'LIMS_POSTGRES_USER=salus_lims_demo' \
    "LIMS_POSTGRES_PASSWORD=$lims_db_password" \
    "LIMS_ADMIN_PASSWORD=$lims_admin_password" > .env
  chmod 600 .env
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

if [[ ! -f config/erp.conf ]]; then
  umask 077
  printf '[options]\nadmin_passwd = %s\naddons_path = /usr/lib/python3/dist-packages/odoo/addons,/mnt/extra-addons\ndb_name = salus_erp_demo\nlist_db = False\nproxy_mode = True\n' \
    "$ERP_ADMIN_PASSWORD" > config/erp.conf
  chmod 600 config/erp.conf
fi

if [[ ! -f config/lims.conf ]]; then
  umask 077
  printf '[options]\nadmin_passwd = %s\naddons_path = /usr/lib/python3/dist-packages/odoo/addons,/mnt/extra-addons\ndb_name = salus_ambiental_demo\nlist_db = False\nproxy_mode = True\n' \
    "$LIMS_ADMIN_PASSWORD" > config/lims.conf
  chmod 600 config/lims.conf
fi

# As imagens oficiais do Odoo executam com UID 100 e GID 101. Os arquivos contêm a
# master password, então ficam legíveis só pelo processo Odoo e root no host.
sudo chown 100:101 config/erp.conf config/lims.conf
sudo chmod 400 config/erp.conf config/lims.conf

echo 'Segredos de demo preparados (valores não exibidos).'
