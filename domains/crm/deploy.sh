#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
bash ./setup-secrets.sh
docker compose config --quiet
docker compose build odoo
docker compose up -d --wait
docker compose ps
