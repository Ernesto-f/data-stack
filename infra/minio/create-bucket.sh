#!/bin/sh
set -e

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER vazio}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD vazio}"

ENDPOINT="${MINIO_ENDPOINT_INTERNAL:-http://127.0.0.1:9000}"

echo ">> Esperando MinIO responder em ${ENDPOINT} ..."

i=0
until mc alias set minio "${ENDPOINT}" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  i=$((i+1))
  if [ $i -gt 60 ]; then
    echo "!! MinIO não respondeu a tempo (120s). Abortando."
    exit 1
  fi
  sleep 2
done

echo ">> MinIO OK. Criando buckets (idempotente)..."
mc mb --ignore-existing minio/fileupload
mc mb --ignore-existing minio/bronze
mc mb --ignore-existing minio/silver
mc mb --ignore-existing minio/gold
mc mb --ignore-existing minio/backups

echo ">> Buckets prontos:"
mc ls minio
