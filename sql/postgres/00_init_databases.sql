-- Chay 1 lan boi entrypoint cua image postgres (chi khi data volume con trong).
-- Tach metadata Airflow khoi audit/observability cua pipeline.

CREATE DATABASE airflow;
CREATE DATABASE pipeline;

COMMENT ON DATABASE airflow  IS 'Airflow metadata (dag run, task instance, xcom...)';
COMMENT ON DATABASE pipeline IS 'Audit/observability cua data pipeline do team tu quan ly';
