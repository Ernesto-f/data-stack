#!/bin/sh
set -e

echo ">> Aguardando MinIO ficar pronto..."
i=0
until mc alias set minio "$MC_HOST_minio" 2>/dev/null; do
  i=$((i+1))
  if [ $i -gt 30 ]; then
    echo "MinIO não respondeu a tempo"; exit 1
  fi
  sleep 2
done

for b in bronze silver gold backups; do
  if mc ls minio/$b >/dev/null 2>&1; then
    echo "Bucket $b já existe"
  else
    echo "Criando bucket $b"
    mc mb minio/$b
  fi
done

echo ">> Buckets prontos."
