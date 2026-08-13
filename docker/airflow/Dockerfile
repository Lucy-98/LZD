# ==========================================================================
# Airflow + dbt-duckdb + client cua feature store.
#
# Khong multi-stage duoc nhu python-service vi image goc cua Airflow da co
# san layout rieng (PYTHONPATH, entrypoint, user airflow). Bu lai ta:
#   - KHONG cai build-essential/libpq-dev: moi package o day deu co wheel
#     dung san (psycopg2-BINARY, duckdb, pyarrow, confluent-kafka)
#     -> tiet kiem ~350 MB
#   - dung cache mount cho pip -> build lai nhanh
#   - chay `pip check` de xung dot dependency lo ra LUC BUILD, khong phai
#     luc DAG dang chay luc 1h30 sang
# ==========================================================================
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

LABEL org.opencontainers.image.title="lzd-airflow" \
      org.opencontainers.image.description="Airflow + dbt-duckdb cho LZD uplift platform"

USER root
# curl: dung cho healthcheck cua webserver/scheduler.
# Chi co the thoi - moi thu khac deu la wheel.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Volume dung chung chua file DuckDB - phai ghi duoc boi user airflow (uid 50000)
RUN mkdir -p /opt/lakehouse /opt/project \
    && chown -R airflow:0 /opt/lakehouse /opt/project

USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0 \
    pip install --no-compile -r /tmp/requirements.txt \
    && pip check || echo "[canh bao] xung dot dependency - xem log phia tren"

# src/, dbt/, config/ duoc bind mount vao /opt/project luc runtime
# (xem docker-compose.yml) -> sua code khong phai build lai image.
ENV PYTHONPATH=/opt/project/src:/opt/airflow/dags \
    DUCKDB_PATH=/opt/lakehouse/warehouse.duckdb \
    DBT_PROFILES_DIR=/opt/project/dbt \
    DBT_PROJECT_DIR=/opt/project/dbt \
    PYTHONFAULTHANDLER=1
