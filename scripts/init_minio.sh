#!/bin/sh
# Tao bucket cho lakehouse / model registry / mlflow artifacts.
# Idempotent: chay lai nhieu lan khong loi.
set -e

echo "[minio-init] cho MinIO san sang..."
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  sleep 2
done

for BUCKET in "$MINIO_BUCKET_LAKE" "$MINIO_BUCKET_MODELS" "$MINIO_BUCKET_MLFLOW"; do
  if mc ls "local/$BUCKET" >/dev/null 2>&1; then
    echo "[minio-init] bucket '$BUCKET' da ton tai -> bo qua"
  else
    mc mb "local/$BUCKET"
    echo "[minio-init] tao bucket '$BUCKET'"
  fi
done

# Layout thu muc cua data lake (medallion)
mc mb -p "local/$MINIO_BUCKET_LAKE/raw/user_snapshot"   >/dev/null 2>&1 || true
mc mb -p "local/$MINIO_BUCKET_LAKE/raw/app_events"      >/dev/null 2>&1 || true
mc mb -p "local/$MINIO_BUCKET_LAKE/exports"             >/dev/null 2>&1 || true

# Version cu cua parquet raw giu 7 ngay cho de debug, roi tu don
mc ilm rule add --expire-days 7 "local/$MINIO_BUCKET_LAKE" --prefix "raw/app_events/" >/dev/null 2>&1 || true

echo "[minio-init] xong."
mc ls local
