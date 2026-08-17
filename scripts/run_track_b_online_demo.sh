#!/bin/sh
set -eu

REPO_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_DIR"

COMPOSE_ENV=${COMPOSE_ENV:-config/demo-stack.env}

echo "[demo] dung stack rieng theo $COMPOSE_ENV"
# inference-api va stream-consumer dung chung mot image/tag. Chi build image
# chung mot lan de BuildKit khong export cung tag tu hai target song song.
docker compose --env-file "$COMPOSE_ENV" build \
  airflow-scheduler inference-api
docker compose --env-file "$COMPOSE_ENV" up -d --wait \
  postgres pgadmin redis redisinsight minio kafka kafka-ui \
  pushgateway statsd-exporter redis-exporter postgres-exporter kafka-exporter \
  prometheus loki promtail grafana inference-api

# Hai init job la one-shot va thoat ma 0 sau khi xong. Khong dua chung vao
# `up --wait` vi Compose xem moi container da thoat la loi, ke ca exit code 0.
docker compose --env-file "$COMPOSE_ENV" run --rm --no-deps minio-init
docker compose --env-file "$COMPOSE_ENV" run --rm --no-deps kafka-init

# Airflow metadata la mot buoc khoi tao bat buoc, khong phai healthcheck. Demo
# cu dung image scheduler lam job one-shot nhung chua tung migrate database,
# dan toi Postgres lap lai `relation log does not exist`. Chay init truoc, sau
# do moi bat webserver/scheduler va bo qua dependency container one-shot da rm.
docker compose --env-file "$COMPOSE_ENV" run --rm --no-deps airflow-init
docker compose --env-file "$COMPOSE_ENV" up -d --no-deps --wait \
  airflow-webserver airflow-scheduler

# Chi bat consumer; KHONG bat event-producer random. Demo runner se publish
# mot lo bounded cua dung 10 user Track B roi thoat.
docker compose --env-file "$COMPOSE_ENV" --profile stream \
  up -d --no-deps --wait stream-consumer

echo "[demo] chay Track A -> Track B -> online flow"
docker compose --env-file "$COMPOSE_ENV" run --rm --no-deps airflow-scheduler \
  python -m lzd_pipeline.demo.track_b_online "$@"

echo "[demo] UI/API"
echo "  FastAPI:      http://localhost:18000/docs"
echo "  Airflow:      http://localhost:18080 (admin/admin)"
echo "  Kafka UI:     http://localhost:18082"
echo "  MinIO:        http://localhost:19001"
echo "  RedisInsight: http://localhost:15540"
echo "  pgAdmin:      http://localhost:15050"
echo "  Grafana:      http://localhost:13000 (admin/admin)"
echo "  Prometheus:   http://localhost:19090"
echo "  Loki ready:   http://localhost:13100/ready"
echo "  Pushgateway:  http://localhost:19091"
