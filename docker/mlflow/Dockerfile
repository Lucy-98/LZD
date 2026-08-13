# ==========================================================================
# MLflow tracking server: backend = Postgres, artifact = MinIO (S3).
#
# Ban truoc cai build-essential + libpq-dev roi giu lai trong image cuoi
# (~350 MB thua) trong khi psycopg2-BINARY khong can compile gi ca.
# Ban nay: multi-stage + khong apt package nao o runtime.
# ==========================================================================
ARG PYTHON_VERSION=3.11.10
ARG MLFLOW_VERSION=2.16.2

# ------------------------------------------------------------- BUILDER ----
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder
ARG MLFLOW_VERSION

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install \
        "mlflow==${MLFLOW_VERSION}" \
        psycopg2-binary==2.9.9 \
        boto3==1.35.36

# ------------------------------------------------------------- RUNTIME ----
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="lzd-mlflow" \
      org.opencontainers.image.description="MLflow tracking server (Postgres backend + MinIO artifacts)"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:${PATH}"

COPY --from=builder /opt/venv /opt/venv

RUN useradd --create-home --uid 10002 --shell /usr/sbin/nologin mlflow \
    && mkdir -p /mlflow && chown -R mlflow:mlflow /mlflow

WORKDIR /mlflow
USER mlflow

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD ["python", "-c", "import urllib.request;urllib.request.urlopen('http://localhost:5000/health',timeout=4)"]

CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
