# ==========================================================================
# Image dung chung cho: event-producer, stream-consumer, inference-api
#
# Multi-stage: stage `builder` cai package vao 1 virtualenv, stage `runtime`
# chi copy virtualenv do sang. Ket qua: compiler, header, pip cache va toan bo
# apt build-deps KHONG di theo image cuoi.
#     1 stage : ~1.05 GB
#     2 stage : ~480 MB
# ==========================================================================
ARG PYTHON_VERSION=3.11.10

# ------------------------------------------------------------- BUILDER ----
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_COMPILE=1 \
    PIP_ROOT_USER_ACTION=ignore

# Chi stage nay moi can compiler (mot so wheel phai build tu source)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY docker/python-service/requirements.txt /tmp/requirements.txt
# Cache mount: build lai sau khi doi 1 dong requirements chi mat vai giay
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel \
    && pip install -r /tmp/requirements.txt \
    && pip check || echo "[canh bao] co xung dot dependency - xem log phia tren"

# ------------------------------------------------------------- RUNTIME ----
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="lzd-python-service" \
      org.opencontainers.image.description="Producer / consumer / inference API cho LZD uplift platform" \
      org.opencontainers.image.source="https://github.com/lzd/uplift-platform"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONPATH=/opt/project/src

# Runtime gan nhu khong can apt package nao:
#   - healthcheck dung urllib cua Python thay vi curl
#   - reaping process do `init: true` trong docker-compose lo
#
# NGOAI TRU `libgomp1`: production artifact 30F dung LightGBM va can OpenMP
# runtime. Artifact duoc mount read-only va kiem checksum luc khoi dong.
#
# ~150 KB. Khong dung `libgomp1-*-dev`: runtime khong can header.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

# Non-root ngay tu dau, va COPY --chown de khong sinh them layer trung file
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser \
    && mkdir -p /opt/project /opt/lakehouse \
    && chown -R appuser:appuser /opt/project /opt/lakehouse

WORKDIR /opt/project

# Code/config duoc bind mount trong docker-compose de tranh bake OneDrive
# reparse-point files vao build context. Standalone image chi chua runtime deps.

USER appuser

# Mac dinh check cong metrics; inference-api override bang healthcheck rieng
# trong docker-compose (goi /health).
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import os,urllib.request;urllib.request.urlopen(f\"http://localhost:{os.environ.get('METRICS_PORT','9105')}/metrics\",timeout=4)"]

# Command that su set trong docker-compose cho tung service
CMD ["python", "-c", "print('Set `command:` trong docker-compose.yml')"]
