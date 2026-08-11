-- Schema audit/observability cua data pipeline.
-- Day la "so cai" de tra loi: chuyen gi da xay ra, luc nao, bao nhieu dong,
-- version nao dang active tren Redis, sync nao that bai o shard nao.
\connect pipeline

CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------------------
-- 1) Moi lan pipeline chay (bat ky stage nao) deu ghi 1 dong o day
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id          TEXT        PRIMARY KEY,          -- <dag_id>__<logical_date>__<try>
    dag_id          TEXT        NOT NULL,
    task_id         TEXT        NOT NULL,
    logical_date    DATE        NOT NULL,
    stage           TEXT        NOT NULL,             -- ingest|transform|train|sync|dq
    status          TEXT        NOT NULL,             -- RUNNING|SUCCESS|FAILED|SKIPPED
    rows_in         BIGINT,
    rows_out        BIGINT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_sec    DOUBLE PRECISION
                    GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (finished_at - started_at))) STORED,
    error_message   TEXT,
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_dag_date ON ops.pipeline_run (dag_id, logical_date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_status   ON ops.pipeline_run (status, started_at DESC);

-- ---------------------------------------------------------------------------
-- 2) Audit rieng cho feature sync DuckDB -> Redis (trai tim cua he thong)
--    1 dong = 1 feature version. Dung de idempotency + recovery + rollback.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.feature_sync_audit (
    feature_version   TEXT        PRIMARY KEY,        -- vd: v20260805 (deterministic theo logical_date)
    logical_date      DATE        NOT NULL,
    source_table      TEXT        NOT NULL,
    status            TEXT        NOT NULL,           -- IN_PROGRESS|VALIDATING|ACTIVE|FAILED|ROLLED_BACK|RETIRED
    total_shards      INT         NOT NULL,
    completed_shards  INT         NOT NULL DEFAULT 0,
    expected_rows     BIGINT,
    written_rows      BIGINT      NOT NULL DEFAULT 0,
    checksum          TEXT,                           -- hash cua (row_count, sum cua vai cot) de so offline vs online
    validation_report JSONB,
    activated_at      TIMESTAMPTZ,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    error_message     TEXT
);
CREATE INDEX IF NOT EXISTS idx_fsa_status ON ops.feature_sync_audit (status, logical_date DESC);

-- Chi tiet tung shard: cho phep rerun chi cac shard FAILED (fault tolerance)
CREATE TABLE IF NOT EXISTS ops.feature_sync_shard (
    feature_version TEXT        NOT NULL REFERENCES ops.feature_sync_audit(feature_version) ON DELETE CASCADE,
    shard_id        INT         NOT NULL,
    status          TEXT        NOT NULL,             -- PENDING|DONE|FAILED
    rows_written    BIGINT      NOT NULL DEFAULT 0,
    attempt         INT         NOT NULL DEFAULT 0,
    duration_ms     BIGINT,
    error_message   TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (feature_version, shard_id)
);

-- ---------------------------------------------------------------------------
-- 3) Ket qua data quality check (freshness, null rate, drift, skew...)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.dq_result (
    id            BIGSERIAL   PRIMARY KEY,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    logical_date  DATE        NOT NULL,
    check_name    TEXT        NOT NULL,
    target        TEXT        NOT NULL,               -- table/topic/redis namespace
    severity      TEXT        NOT NULL DEFAULT 'ERROR', -- INFO|WARN|ERROR
    passed        BOOLEAN     NOT NULL,
    observed      DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    details       JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_dq_result_time ON ops.dq_result (checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_result_name ON ops.dq_result (check_name, checked_at DESC);

-- ---------------------------------------------------------------------------
-- 4) Log inference request (mau) - de sau nay do training/serving skew that su
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.inference_log (
    id                BIGSERIAL   PRIMARY KEY,
    requested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_id           TEXT        NOT NULL,
    feature_version   TEXT,
    model_version     TEXT,
    uplift_score      DOUBLE PRECISION,
    decision          TEXT,                            -- SEND_VOUCHER|NO_VOUCHER
    latency_ms        DOUBLE PRECISION,
    cache_hit         BOOLEAN,
    features_missing  INT         NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_inference_log_time ON ops.inference_log (requested_at DESC);

-- ---------------------------------------------------------------------------
-- View tien cho Grafana (datasource postgres)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ops.v_latest_sync AS
SELECT feature_version, logical_date, status, total_shards, completed_shards,
       expected_rows, written_rows,
       ROUND(100.0 * completed_shards / NULLIF(total_shards, 0), 1) AS pct_shards,
       started_at, finished_at, activated_at, error_message
FROM ops.feature_sync_audit
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW ops.v_dq_last_24h AS
SELECT check_name, target, severity,
       COUNT(*) FILTER (WHERE passed)       AS passed_cnt,
       COUNT(*) FILTER (WHERE NOT passed)   AS failed_cnt,
       MAX(checked_at)                      AS last_check
FROM ops.dq_result
WHERE checked_at > now() - INTERVAL '24 hours'
GROUP BY 1, 2, 3;

GRANT ALL ON SCHEMA ops TO CURRENT_USER;
