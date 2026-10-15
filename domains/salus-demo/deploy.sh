#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
bash ./setup-secrets.sh

set -a
# shellcheck disable=SC1091
source .env
set +a

docker compose config --quiet
docker compose build
docker compose up -d --wait erp-postgres lims-postgres

erp_modules='salus_erp_core,salus_crm,salus_erp_sales,salus_erp_project,salus_whatsapp'
lims_modules='salus_core,salus_ambiental,izata_design_system,izata_theme_studio'

echo '==> Instalando/atualizando SALUS ERP em banco sem demo data do Odoo.'
docker compose run --rm -T --no-deps salus-erp \
  odoo -d salus_erp_demo -i "$erp_modules" -u "$erp_modules" \
  --without-demo=all --stop-after-init --logfile=- </dev/null

echo '==> Instalando/atualizando SALUS Ambiental em banco sem registros de exemplo.'
docker compose run --rm -T --no-deps salus-backoffice \
  odoo -d salus_ambiental_demo -i "$lims_modules" -u "$lims_modules" \
  --without-demo=all --stop-after-init --logfile=- </dev/null

echo '==> Criando poucos registros fictícios do demo ERP (script idempotente).'
docker compose run --rm -T --no-deps salus-erp \
  odoo shell -d salus_erp_demo < ./salus-erp/scripts/seed_demo.py

docker compose up -d --wait
docker compose ps
echo 'Stacks SALUS demo prontas. O login Odoo inicial é admin; altere-o após o primeiro acesso.'
