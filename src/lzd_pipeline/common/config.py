"""Cau hinh tap trung - doc tu bien moi truong (12-factor).

Khong hardcode host/port o bat ky cho nao khac trong code.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


from pathlib import Path

_LOCAL_ROOT = Path(__file__).resolve().parents[3]


def _default_spec_path() -> str:
    opt_path = "/opt/project/config/features/feature_spec.yml"
    if os.path.exists(opt_path):
        return opt_path
    return str(_LOCAL_ROOT / "config" / "features" / "feature_spec.yml")


def _default_redis_host() -> str:
    if "REDIS_HOST" in os.environ:
        return os.environ["REDIS_HOST"]
    if os.path.exists("/.dockerenv") or (os.path.exists("/opt/project/src") and not os.path.exists(str(_LOCAL_ROOT))):
        return "redis"
    return "localhost"


def _default_kafka_servers() -> str:
    if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
        return os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if os.path.exists("/.dockerenv") or (os.path.exists("/opt/project/src") and not os.path.exists(str(_LOCAL_ROOT))):
        return "kafka:9092"
    return "localhost:29092"


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", _default_kafka_servers()))
    topic_events: str = field(default_factory=lambda: _env("KAFKA_TOPIC_EVENTS", "app.user.events.v1"))
    topic_dlq: str = field(default_factory=lambda: _env("KAFKA_TOPIC_DLQ", "app.user.events.dlq.v1"))
    consumer_group: str = field(default_factory=lambda: _env("KAFKA_CONSUMER_GROUP", "feature-stream-consumer"))


@dataclass(frozen=True)
class RedisConfig:
    host: str = field(default_factory=lambda: _env("REDIS_HOST", _default_redis_host()))
    port: int = field(default_factory=lambda: _env_int("REDIS_PORT", 6379))
    db: int = field(default_factory=lambda: _env_int("REDIS_DB", 0))

    @property
    def url(self) -> str:
        return f"redis://{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str = field(default_factory=lambda: _env("MINIO_ENDPOINT", "http://minio:9000"))
    access_key: str = field(default_factory=lambda: _env("MINIO_ROOT_USER", "minioadmin"))
    secret_key: str = field(default_factory=lambda: _env("MINIO_ROOT_PASSWORD", "minioadmin123"))
    bucket_lake: str = field(default_factory=lambda: _env("MINIO_BUCKET_LAKE", "lakehouse"))
    bucket_models: str = field(default_factory=lambda: _env("MINIO_BUCKET_MODELS", "models"))

    @property
    def host_no_scheme(self) -> str:
        return self.endpoint.replace("http://", "").replace("https://", "")

    @property
    def use_ssl(self) -> bool:
        return self.endpoint.startswith("https://")


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _env("PG_HOST", "postgres"))
    port: int = field(default_factory=lambda: _env_int("PG_PORT", 5432))
    user: str = field(default_factory=lambda: _env("PG_USER", "lzd"))
    password: str = field(default_factory=lambda: _env("PG_PASSWORD", "lzd_secret"))
    database: str = field(default_factory=lambda: _env("PG_PIPELINE_DB", "pipeline"))

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} user={self.user} "
            f"password={self.password} dbname={self.database}"
        )


@dataclass(frozen=True)
class FeatureStoreConfig:
    spec_path: str = field(default_factory=lambda: _env("FEATURE_SPEC_PATH", _default_spec_path()))
    shards: int = field(default_factory=lambda: _env_int("FEATURE_SYNC_SHARDS", 32))
    batch_size: int = field(default_factory=lambda: _env_int("FEATURE_SYNC_BATCH_SIZE", 1000))
    versions_to_keep: int = field(default_factory=lambda: _env_int("FEATURE_VERSIONS_TO_KEEP", 2))
    stale_ttl_seconds: int = field(default_factory=lambda: _env_int("FEATURE_STALE_TTL_SECONDS", 86400))
    realtime_ttl_seconds: int = field(default_factory=lambda: _env_int("REALTIME_OVERLAY_TTL_SECONDS", 3600))
    validation_sample: int = field(default_factory=lambda: _env_int("FEATURE_VALIDATION_SAMPLE", 500))


@dataclass(frozen=True)
class Settings:
    service_name: str = field(default_factory=lambda: _env("SERVICE_NAME", "lzd-pipeline"))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    duckdb_path: str = field(default_factory=lambda: _env("DUCKDB_PATH", "/opt/lakehouse/warehouse.duckdb"))
    lake_root: str = field(default_factory=lambda: _env("LAKE_ROOT", "s3://lakehouse"))
    pushgateway_url: str = field(default_factory=lambda: _env("PUSHGATEWAY_URL", "http://pushgateway:9091"))
    metrics_port: int = field(default_factory=lambda: _env_int("METRICS_PORT", 9105))
    mlflow_tracking_uri: str = field(default_factory=lambda: _env("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow_experiment: str = field(default_factory=lambda: _env("MLFLOW_EXPERIMENT", "uplift-descn"))
    uplift_threshold: float = field(default_factory=lambda: _env_float("UPLIFT_DECISION_THRESHOLD", 0.007190971))

    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    minio: MinioConfig = field(default_factory=MinioConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    feature_store: FeatureStoreConfig = field(default_factory=FeatureStoreConfig)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
