#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
if [[ ! -f .env ]]; then
  umask 077
  db_password="$(openssl rand -hex 32)"
  printf '%s\n' \
    'TRACCAR_DB_USER=pulse_tracking' \
    "TRACCAR_DB_PASSWORD=$db_password" > .env
  chmod 600 .env
fi
echo 'Credenciais da base preparadas (valores não exibidos).'
