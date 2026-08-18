#!/usr/bin/env bash
# ==============================================================================
# Dieu khien stack LZD Uplift tren macOS / Linux.
# Tuong duong scripts/stack.ps1 tren Windows.
#
# Cach dung:
#   ./scripts/stack.sh up          # = docker compose up -d (ca nen tang)
#   ./scripts/stack.sh up-all      # them event-producer + stream-consumer
#   ./scripts/stack.sh up-core     # chi ha tang + airflow + api, bo observability
#   ./scripts/stack.sh doctor      # chan doan Docker daemon, proxy, network
#   ./scripts/stack.sh init        # tao .env + tao thu muc + build truoc
#   ./scripts/stack.sh status      # trang thai container + link UI
#   ./scripts/stack.sh logs <svc>  # xem log cua mot service
#   ./scripts/stack.sh health      # kiem tra endpoint suc khoe
#   ./scripts/stack.sh redis       # mo redis-cli
#   ./scripts/stack.sh duckdb      # liet ke bang DuckDB qua container Airflow
#   ./scripts/stack.sh reconstruction [target] # chay Track A -> Gate -> T0 -> Track B
#   ./scripts/stack.sh track-a [N] # gen Track A raw events tu full_trainset.csv
#   ./scripts/stack.sh snapshot    # ghi reconstruction snapshot vao docs/
#   ./scripts/stack.sh test        # chay unit test qua pytest
#   ./scripts/stack.sh down        # dung (giu du lieu)
#   ./scripts/stack.sh reset       # dung + XOA het volume
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# ANSI Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

CORE_SERVICES=(
  postgres redis minio minio-init kafka kafka-init
  airflow-init airflow-webserver airflow-scheduler
  mlflow inference-api
)

if [ -f ".venv/bin/python3" ]; then
  PYTHON_CMD=".venv/bin/python3"
elif [ -f ".venv/bin/python" ]; then
  PYTHON_CMD=".venv/bin/python"
else
  PYTHON_CMD="python3"
fi

function section() {
  echo ""
  echo -e "${CYAN}=== $1 ===${NC}"
}

function ensure_dirs() {
  mkdir -p airflow/logs airflow/plugins data .tmp
  # Tren macOS / Linux can cap quyen group ghi cho bind mount airflow/logs
  chmod -R g+w airflow/logs airflow/plugins 2>/dev/null || true
}

function cmd_doctor() {
  section "Chan doan moi truong Docker & He thong (macOS / Linux)"
  
  echo -n "Docker daemon: "
  if docker info >/dev/null 2>&1; then
    echo -e "${GREEN}DANG CHAY${NC}"
  else
    echo -e "${RED}KHONG KET NOI DUOC${NC}"
    echo -e "${YELLOW}Vui long mo Docker Desktop va doi dong co khoi dong xong.${NC}"
    exit 1
  fi

  echo -n "Docker Compose: "
  if docker compose version >/dev/null 2>&1; then
    echo -e "${GREEN}SAN SANG ($(docker compose version --short))${NC}"
  else
    echo -e "${RED}CHUA CAI${NC}"
    exit 1
  fi

  echo -n "Python 3: "
  if command -v python3 >/dev/null 2>&1; then
    echo -e "${GREEN}$(python3 --version)${NC}"
  else
    echo -e "${RED}KHONG TIM THAY${NC}"
  fi

  echo ""
  echo "Thong so he thong Docker:"
  docker info --format '  - CPU: {{.NCPU}} cores' 2>/dev/null || true
  docker info --format '  - RAM: {{.MemTotal}}' 2>/dev/null || true
  docker info --format '  - Architecture: {{.Architecture}}' 2>/dev/null || true
  docker info --format '  - OS: {{.OperatingSystem}}' 2>/dev/null || true
  echo ""
  echo "Kiem tra cac image co san:"
  docker images --format "  - {{.Repository}}:{{.Tag}} ({{.Size}})" | head -n 10
}

function cmd_init() {
  section "Khoi tao moi truong"
  if [ ! -f .env ]; then
    echo "Sao chep .env.example -> .env"
    cp .env.example .env
  else
    echo ".env da ton tai, bo qua sao chep."
  fi
  ensure_dirs
  echo "Build tat ca image..."
  docker compose --profile all build
}

function cmd_build() {
  section "Build images"
  docker compose --profile all build
}

function cmd_up() {
  section "Khoi dong toan bo stack"
  ensure_dirs
  docker compose up -d
  cmd_status
}

function cmd_up_core() {
  section "Khoi dong Core Services (bo observability)"
  ensure_dirs
  docker compose up -d "${CORE_SERVICES[@]}"
  cmd_status
}

function cmd_up_all() {
  section "Khoi dong toan bo stack + realtime stream"
  ensure_dirs
  docker compose --profile all up -d
  cmd_status
}

function cmd_down() {
  section "Dung stack (giu du lieu)"
  docker compose --profile all down
}

function cmd_reset() {
  section "Reset stack (XOA VOLUME & LOGS)"
  docker compose --profile all down -v
  rm -rf airflow/logs/*
  echo -e "${GREEN}Da reset toan bo volume va log.${NC}"
}

function cmd_status() {
  section "Trang thai Containers"
  docker compose --profile all ps
  echo ""
  echo -e "${GREEN}Cac cong dich vu:${NC}"
  echo "  Airflow       http://localhost:8080   (admin/admin)"
  echo "  Grafana       http://localhost:3000   (admin/admin)"
  echo "  Kafka UI      http://localhost:8082"
  echo "  MinIO         http://localhost:9001   (minioadmin/minioadmin123)"
  echo "  MLflow        http://localhost:5001"
  echo "  Prometheus    http://localhost:9090"
  echo "  RedisInsight  http://localhost:5540"
  echo "  Inference API http://localhost:8000/docs"
}

function cmd_logs() {
  local service="$1"
  if [ -z "$service" ]; then
    docker compose logs -f --tail=200
  else
    docker compose logs -f --tail=200 "$service"
  fi
}

function cmd_health() {
  section "Kiem tra endpoint suc khoe"
  local endpoints=(
    "http://localhost:8080/health"
    "http://localhost:3000/api/health"
    "http://localhost:9090/-/healthy"
    "http://localhost:9000/minio/health/live"
    "http://localhost:3100/ready"
    "http://localhost:8000/health"
  )
  for url in "${endpoints[@]}"; do
    printf "%-45s" "$url"
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}\n" "$url" || echo "LOI")
    if [ "$code" == "200" ]; then
      echo -e "${GREEN}${code}${NC}"
    else
      echo -e "${RED}${code}${NC}"
    fi
  done
}

function cmd_redis() {
  docker compose exec redis redis-cli
}

function cmd_duckdb() {
  docker compose exec airflow-scheduler python -c "from lzd_pipeline.common.clients import duckdb_conn; con=duckdb_conn(read_only=True).__enter__(); print(con.execute('SHOW ALL TABLES').fetchdf())"
}

function cmd_test() {
  section "Chay Unit / Contract Tests"
  if [ -n "$1" ]; then
    "$PYTHON_CMD" -m pytest "$1" -v
  else
    "$PYTHON_CMD" -m pytest tests/ -v
  fi
}

function cmd_reconstruction() {
  section "Chay Reconstruction E2E Pipeline"
  local target="$1"
  if [ -n "$target" ]; then
    PYTHONPATH=src "$PYTHON_CMD" -m lzd_pipeline.reconstruction.e2e --target "$target"
  else
    PYTHONPATH=src "$PYTHON_CMD" -m lzd_pipeline.reconstruction.e2e
  fi
}

function cmd_track_a() {
  section "Chay Track A Batch"
  local limit="${1:-1000}"
  PYTHONPATH=src "$PYTHON_CMD" -m lzd_pipeline.reconstruction.track_a_batch --limit "$limit" --verify-limit 50
}

function cmd_snapshot() {
  section "Ghi Reconstruction Snapshot"
  PYTHONPATH=src "$PYTHON_CMD" -m lzd_pipeline.reconstruction.snapshot
}

COMMAND="${1:-status}"
shift || true

case "$COMMAND" in
  doctor) cmd_doctor ;;
  init) cmd_init ;;
  build) cmd_build ;;
  up) cmd_up ;;
  up-core) cmd_up_core ;;
  up-all) cmd_up_all ;;
  down) cmd_down ;;
  reset) cmd_reset ;;
  status|ps) cmd_status ;;
  logs) cmd_logs "$@" ;;
  health) cmd_health ;;
  redis) cmd_redis ;;
  duckdb) cmd_duckdb ;;
  test) cmd_test ;;
  reconstruction) cmd_reconstruction "$@" ;;
  track-a) cmd_track_a "$@" ;;
  snapshot) cmd_snapshot ;;
  help|-h|--help)
    echo "Cach dung: $0 [command]"
    echo "Commands: up, up-core, up-all, down, reset, status, logs, health, build, init, doctor, redis, duckdb, test, reconstruction, track-a, snapshot"
    ;;
  *)
    echo -e "${RED}Lenh khong hop le: ${COMMAND}${NC}"
    echo "Chay '$0 help' de xem danh sach lenh."
    exit 1
    ;;
esac
