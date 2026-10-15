#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: %s <tenant-key>\n' "$0" >&2
  printf 'Optional: CERTBOT_EMAIL=admin@example.com %s <tenant-key>\n' "$0" >&2
  exit 2
}

tenant_key="${1:-}"
if [[ ! "$tenant_key" =~ ^[a-z0-9][a-z0-9-]{1,29}[a-z0-9]$ ]]; then
  usage
fi

if [[ $# -ne 1 ]]; then
  usage
fi

cd "$(dirname "$0")"
tenant_env="tenants/$tenant_key/.env"
vhost_source="tenants/$tenant_key/nginx.conf"

if [[ ! -f "$tenant_env" || ! -f "$vhost_source" ]]; then
  printf 'Tenant %s is not provisioned; run setup-secrets.sh first.\n' "$tenant_key" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$tenant_env"
set +a

: "${CRM_PUBLIC_HOST:?CRM_PUBLIC_HOST is missing from $tenant_env}"
expected_host="${tenant_key}.crm.izatacore.com"
if [[ "$tenant_key" == "demo" ]]; then
  expected_host="democrm.izatacore.com"
fi
if [[ "$CRM_PUBLIC_HOST" != "$expected_host" ]]; then
  printf 'Expected CRM_PUBLIC_HOST=%s, got %s.\n' "$expected_host" "$CRM_PUBLIC_HOST" >&2
  exit 1
fi

command -v sudo >/dev/null || { printf 'sudo is required.\n' >&2; exit 1; }
command -v certbot >/dev/null || { printf 'certbot is required.\n' >&2; exit 1; }
sudo -v

host="$CRM_PUBLIC_HOST"
available="/etc/nginx/sites-available/$host.conf"
enabled="/etc/nginx/sites-enabled/$host.conf"
backup_root="/var/backups/pulse-crm-vhosts"
stamp="$(date +%Y%m%d-%H%M%S)"
backup="$backup_root/$host.$stamp.conf"
temporary_vhost="$(mktemp)"
trap 'rm -f "$temporary_vhost"' EXIT

had_available=0
if sudo test -e "$available" || sudo test -L "$available"; then
  had_available=1
  sudo install -d -m 700 "$backup_root"
  sudo cp -a "$available" "$backup"
fi

had_enabled=0
previous_enabled_target=""
if sudo test -e "$enabled" || sudo test -L "$enabled"; then
  had_enabled=1
  previous_enabled_target="$(sudo readlink "$enabled" || true)"
fi

restore_previous() {
  if [[ "$had_available" == 1 ]]; then
    sudo cp -a "$backup" "$available"
  else
    sudo rm -f "$available"
  fi
  if [[ "$had_enabled" == 1 ]]; then
    sudo ln -sfn "$previous_enabled_target" "$enabled"
  else
    sudo rm -f "$enabled"
  fi
  sudo nginx -t >/dev/null 2>&1 && sudo systemctl reload nginx >/dev/null 2>&1 || true
}

install_config() {
  sudo install -o root -g root -m 644 "$1" "$available"
  sudo ln -sfn "$available" "$enabled"
}

if ! sudo test -s "/etc/letsencrypt/live/$host/fullchain.pem" \
  || ! sudo test -s "/etc/letsencrypt/live/$host/privkey.pem"; then
  cat >"$temporary_vhost" <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name $host;

    location ^~ /.well-known/acme-challenge/ {
        root /var/www/html;
        try_files \$uri =404;
    }

    location / { return 404; }
}
EOF

  if ! install_config "$temporary_vhost" || ! sudo nginx -t || ! sudo systemctl reload nginx; then
    restore_previous
    printf 'Could not install the temporary HTTP vhost for %s.\n' "$host" >&2
    exit 1
  fi

  certbot_args=(
    certonly
    --webroot
    -w /var/www/html
    -d "$host"
    --non-interactive
    --agree-tos
    --keep-until-expiring
  )
  if [[ -n "${CERTBOT_EMAIL:-}" ]]; then
    certbot_args+=(--email "$CERTBOT_EMAIL")
  else
    certbot_args+=(--register-unsafely-without-email)
  fi

  if ! sudo certbot "${certbot_args[@]}"; then
    restore_previous
    printf 'Certbot failed for %s. Previous Nginx configuration restored.\n' "$host" >&2
    exit 1
  fi
fi

if ! install_config "$vhost_source" || ! sudo nginx -t || ! sudo systemctl reload nginx; then
  restore_previous
  printf 'Could not activate the TLS vhost for %s. Previous configuration restored.\n' "$host" >&2
  exit 1
fi

printf 'Vhost and TLS certificate installed for %s.\n' "$host"
if [[ "$had_available" == 1 ]]; then
  printf 'Previous configuration backup: %s\n' "$backup"
fi
