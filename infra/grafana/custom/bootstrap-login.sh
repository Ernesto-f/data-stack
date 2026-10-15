#!/usr/bin/env bash
set -euo pipefail

SRC="/opt/custom/public/img"
DST="/usr/share/grafana/public/img"

echo "[custom] Copying custom Grafana images..."
ls -la "${SRC}" || true

# Copia arquivos um a um (evita comportamento estranho de glob/cp)
if [ -d "${SRC}" ]; then
  find "${SRC}" -maxdepth 1 -type f \( -name "*.png" -o -name "*.svg" \) -print0 \
    | while IFS= read -r -d '' file; do
        base="$(basename "$file")"
        # sobrescreve sem erro; se falhar por qualquer motivo, não derruba o container
        cp -f "$file" "${DST}/${base}" 2>/dev/null || true
      done
fi

# Força o hashed a usar o SVG "genérico" (opcional)
if [ -f "${SRC}/grafana_icon.svg" ]; then
  for f in "${DST}"/grafana_icon.*.svg; do
    [ -f "$f" ] && cp -f "${SRC}/grafana_icon.svg" "$f" 2>/dev/null || true
  done
fi

if [ -f "${SRC}/g8_login_dark.svg" ]; then
  for f in "${DST}"/g8_login_dark.*.svg; do
    [ -f "$f" ] && cp -f "${SRC}/g8_login_dark.svg" "$f" 2>/dev/null || true
  done
fi

echo "[custom] Done. Starting Grafana..."
exec /run.sh
