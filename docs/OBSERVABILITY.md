# Observability

> **Scope:** sức khỏe data pipeline và reconstruction. API latency, request tracing,
> cache hit và downstream state internals nằm ngoài scope.

## Tín hiệu chính

| Thành phần | Tín hiệu cần theo dõi |
|---|---|
| Kafka | producer rate, consumer lag, DLQ rate |
| Stream consumer | batch size, flush duration, validation failures |
| MinIO | files/rows written, write failures, partition freshness |
| dbt/DuckDB | model duration, test failures, source freshness, writer lock |
| Airflow | DAG/task success, duration, retry, schedule delay |
| Reconstruction | solved/quarantined, Gate A-F, diff count, events/target |
| Provenance | missing lineage, cycle, invalid source transition |

## Logs

Mọi service dùng structured logging. Các field tối thiểu:

```text
service, event, level, timestamp, run_id, target_id, generation_run_id
```

Các event quan trọng:

- `batch_flushed`, `batch_flush_failed`;
- `dlq_write_failed`, `event_invalid`;
- `duckdb_write_open`;
- reconstruction `SOLVED`, `QUARANTINED`, gate failure;
- provenance cycle hoặc contamination failure.

Không log feature payload đầy đủ, label, treatment hoặc credential.

## Metrics Reconstruction

| Metric | Ý nghĩa |
|---|---|
| `reconstruction_targets_total{status,branch}` | số target theo kết quả |
| `reconstruction_gate_failures_total{gate,column}` | lỗi gate |
| `reconstruction_events_total{track,source_type}` | volume event sinh |
| `reconstruction_duration_seconds{stage}` | decode/solve/dbt/gate/handoff |
| `reconstruction_quarantine_ratio` | tỷ lệ không giải được |
| `reconstruction_diff_total{regime,column}` | số cột lệch target |

E2E prototype hiện trả summary qua CLI/Airflow log. Khi nối production, các metric
trên mới được đưa vào Prometheus; không cần phụ thuộc API layer.

## Dashboard

Dashboard data pipeline nên trả lời bốn câu hỏi:

1. Kafka có backlog hay DLQ tăng không?
2. MinIO có nhận object mới trước khi offset tăng không?
3. dbt models và tests có xanh, dữ liệu có fresh không?
4. Reconstruction pass Gate A-F ở tỷ lệ nào, quarantine vì lý do gì?

## Alert tối thiểu

| Alert | Điều kiện gợi ý |
|---|---|
| `KafkaConsumerLagHigh` | lag tăng liên tục 15 phút |
| `LakeWriteFailure` | có `batch_flush_failed` do MinIO |
| `RawPartitionStale` | không có partition mới quá SLA |
| `DbtTestFailure` | bất kỳ critical test fail |
| `ReconstructionGateFailure` | Gate A/B/C/D/E fail |
| `ReconstructionQuarantineHigh` | quarantine vượt ngưỡng pilot đã chốt |
| `ProvenanceViolation` | cycle, missing root hoặc invalid source type |

## Debug nhanh

```bash
make health
make logs s=stream-consumer
docker compose logs airflow-scheduler --tail 200
```

Reconstruction:

```bash
PYTHONPATH=src python3 -m lzd_pipeline.reconstruction.e2e
# hoặc: ./scripts/stack.sh reconstruction
```

Output phải cho thấy từng Gate A-F. Nếu fail, điều tra source event, dbt output và
provenance; không chỉnh target/tolerance để làm dashboard xanh.
