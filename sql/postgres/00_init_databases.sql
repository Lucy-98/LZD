-- Chay 1 lan boi entrypoint cua image postgres (chi khi data volume con trong).
-- Tach 3 database de metadata cua tung he thong khong dam nhau.

CREATE DATABASE airflow;
CREATE DATABASE mlflow;
CREATE DATABASE pipeline;

COMMENT ON DATABASE airflow  IS 'Airflow metadata (dag run, task instance, xcom...)';
COMMENT ON DATABASE mlflow   IS 'MLflow tracking store (experiment, run, metric, model registry)';
COMMENT ON DATABASE pipeline IS 'Audit/observability cua data pipeline do team tu quan ly';
