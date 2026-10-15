#!/bin/sh
set -e

# Sobe MinIO em background
minio "$@" &
PID="$!"

echo ">> MinIO starting..."

export MINIO_ENDPOINT_INTERNAL="http://127.0.0.1:9000"

# Executa bootstrap (idempotente)
# (cria buckets e faz alias)
echo ">> Running bootstrap..."
/usr/local/bin/create-bucket.sh

# Mantém o container vivo (MinIO em foreground via wait)
wait "$PID"
