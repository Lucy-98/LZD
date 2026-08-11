# Observability — vì sao cần từng ấy thứ

## Câu hỏi thường gặp: "sao không chỉ Grafana thôi?"

**Grafana không thu thập và không lưu dữ liệu.** Nó chỉ là lớp hiển thị + query.
Muốn có biểu đồ thì phải có ai đó (a) sinh ra số liệu, (b) lưu số liệu lại.

```
  ai sinh ra?              ai lưu?              ai vẽ?          ai thao tác?
 ─────────────────      ──────────────       ───────────      ───────────────
 service Python  ──┐
 exporters       ──┼──►  Prometheus   ──┐
 Airflow statsd  ──┘                    ├──►  Grafana      Kafka UI
                                        │                  MinIO Console
 container logs ──► Promtail ──► Loki ──┘                  RedisInsight
                                        │                  Airflow UI
 bảng ops.*  ───────► Postgres ─────────┘                  MLflow UI
```

| Thành phần | Nếu bỏ đi thì mất gì |
|---|---|
| **Prometheus** | Grafana không còn nguồn số liệu nào → mọi biểu đồ metric trắng |
| **Loki + Promtail** | mất toàn bộ panel log, phải `docker logs` từng container |
| **Pushgateway** | task Airflow chạy vài giây rồi thoát → Prometheus scrape không kịp → mất metric của batch job |
| **statsd-exporter** | mất metric nội bộ Airflow (task pass/fail, pool slot, scheduler heartbeat) |
| **redis/kafka/postgres exporter** | không biết RAM Redis, consumer lag, connection Postgres |
| **Kafka UI** | không đọc được nội dung message, không xem được DLQ |
| **MinIO Console** | không duyệt/tải được file parquet để kiểm tra bằng mắt |
| **RedisInsight** | không soi được key `fs:*` khi nghi ngờ sync sai |

Prometheus + Loki là **bắt buộc**. Các UI còn lại phục vụ *thao tác* — thứ
Grafana không làm được (Grafana không cho bạn tải file parquet hay xoá một key Redis).

---

## Ba đường metric khác nhau (và vì sao)

### 1. Service chạy liên tục → tự expose `/metrics`

`event-producer`, `stream-consumer`, `inference-api` mở sẵn HTTP endpoint,
Prometheus scrape 15 giây/lần.

```python
from lzd_pipeline.common.metrics import EVENTS_CONSUMED, start_metrics_server
start_metrics_server(9106)
EVENTS_CONSUMED.labels(event_type="order").inc()
```

### 2. Task Airflow (chạy rồi thoát) → push lên Pushgateway

Task sống 30 giây rồi chết — Prometheus scrape 15s/lần sẽ hụt.
Nên task chủ động **đẩy** metric ra và Pushgateway giữ hộ.

```python
from lzd_pipeline.common.metrics import push_batch_metrics
push_batch_metrics("feature_sync",
                   {"feature_sync_written_rows": 1_200_000},
                   labels={"feature_version": "v20260805"})
```

### 3. Trạng thái nghiệp vụ → bảng `ops.*` trong Postgres

Có thứ không hợp làm metric: "shard 17 fail vì lỗi gì", "báo cáo validate gồm
những user nào lệch". Những cái đó ghi vào Postgres, Grafana đọc bằng SQL
(datasource **Pipeline-Postgres**), đồng thời postgres-exporter biến phần tổng
hợp thành metric.

| Bảng | Nội dung |
|---|---|
| `ops.pipeline_run` | mỗi task một dòng: rows_in/out, duration, error |
| `ops.feature_sync_audit` | mỗi feature version một dòng: status, checksum, validation_report |
| `ops.feature_sync_shard` | mỗi shard một dòng → biết chính xác shard nào hỏng |
| `ops.dq_result` | kết quả từng data quality check |
| `ops.inference_log` | mẫu request inference: version, score, latency, cache_hit |

---

## Log: JSON có cấu trúc, không phải text thường

Mọi service dùng `lzd_pipeline.common.logging_setup`. Mỗi dòng log là JSON:

```json
{"ts":"2026-08-05T01:31:22","level":"INFO","logger":"lzd_pipeline.features.sync",
 "service":"airflow","message":"ghi shard xong","event":"shard_written",
 "feature_version":"v20260805","shard_id":17,"rows":37421,"duration_ms":842}
```

Promtail parse JSON, gắn label `level` / `event` / `feature_version`.
Trong Grafana → Explore → Loki:

```logql
{job="docker"} | json | event="shard_written"
{job="docker"} | json | feature_version="v20260805" | level="ERROR"
{component="stream-consumer"} | json | event="batch_flush_failed"
{job="airflow-tasks", dag_id="40_sync_features_to_redis", task_id=~"sync_shard.*"}
```

Trường `event` là **tên sự kiện ổn định** — dùng nó để lọc thay vì bám vào câu chữ:

| `event` | Xảy ra khi |
|---|---|
| `sync_prepare` | mở phiên sync |
| `shard_written` / `shard_skipped` / `shard_failed` | từng shard |
| `sync_validated` | xong bước validate |
| `version_activated` / `version_rollback` | đổi con trỏ |
| `version_expired` / `version_deleted` | GC |
| `batch_flushed` / `batch_flush_failed` | micro-batch của consumer |
| `decision` | mỗi quyết định phát voucher |
| `dq_check` | mỗi data quality check |
| `task_failed` | bất kỳ task Airflow nào fail |

---

## Alert (Prometheus, `config/prometheus/alerts.yml`)

| Alert | Ngưỡng | Nghĩa là gì |
|---|---|---|
| `FeatureStoreStale` | age > 26h | sync hằng ngày đã chết |
| `FeatureSyncFailed` | status=FAILED | xem `ops.feature_sync_shard` |
| `OnlineOfflineMismatch` | mismatch > 0.1% | **nguy cơ training/serving skew** |
| `KafkaConsumerLagHigh` | lag > 50k | consumer không theo kịp |
| `DLQGrowing` | > 1 msg/s | schema đổi hoặc producer bug |
| `InferenceLatencyP99High` | p99 > 100ms | vi phạm SLA nghiệp vụ |
| `FeatureLookupLatencyHigh` | p99 > 10ms | Redis chậm bất thường |
| `FeatureCacheMissHigh` | miss > 5% | nhiều user chưa có feature |

Xem trạng thái: Prometheus → **Alerts** (http://localhost:9090/alerts).
Chưa nối Alertmanager (không cần cho môi trường local) — muốn gửi Slack/email
thì thêm service `alertmanager` và trỏ `alerting:` trong `prometheus.yml`.

---

## Lỗi hiện ra ở đâu — bảng tra đầy đủ

Nguyên tắc: **mỗi loại lỗi phải nhìn thấy được ở ít nhất 2 nơi** (một nơi để
báo động, một nơi để tìm nguyên nhân).

### Lỗi tầng ingestion

| Lỗi | Báo động ở | Tìm nguyên nhân ở |
|---|---|---|
| Event sai schema | metric `lzd_events_dlq_total{reason}` · alert `DLQGrowing` | Kafka UI → topic `.dlq.v1` → header `reason` chứa lý do chính xác |
| Producer chết | `lzd_events_produced_total` phẳng · healthcheck container UNHEALTHY | `docker compose logs event-producer` |
| Consumer không kịp | `kafka_consumergroup_lag` · alert `KafkaConsumerLagHigh` | Grafana 02 → *Lag theo partition* (một partition lệch = skew theo `user_id`) |
| Ghi MinIO fail | log `event="batch_flush_failed"` | offset **chưa commit** → batch được đọc lại, không mất dữ liệu |
| Độ trễ đầu-cuối tăng | `lzd_event_end_to_end_lag_seconds` | Grafana 02 → *batch size & flush time* |

### Lỗi tầng transform (dbt)

| Lỗi | Báo động ở | Tìm nguyên nhân ở |
|---|---|---|
| Model dbt build fail | task `dbt_run` đỏ | Airflow task log (giữ nguyên output dbt) · Loki `{dag_id="20_build_features_dbt"}` |
| dbt test fail | `ops.dq_result` + metric `lzd_dbt_tests_failed` (task `publish_dbt_results` chạy cả khi test fail) | **các dòng vi phạm cụ thể** nằm trong `dq_failures.<tên_test>` của DuckDB (`store_failures: true`) |
| Mart lệch `feature_spec.yml` | task `assert_spec_contract` đỏ, thông báo liệt kê đúng cột thiếu | so `dbt/models/marts/feat_user_serving.sql` với `config/features/feature_spec.yml` |
| DuckDB "Could not set lock" | task log | 2 task cùng ghi → kiểm tra pool `duckdb_writer` = 1 slot (Grafana 03) |

### Lỗi tầng feature store (quan trọng nhất)

| Lỗi | Báo động ở | Tìm nguyên nhân ở |
|---|---|---|
| Shard ghi fail | `ops.feature_sync_shard.status='FAILED'` + `error_message` | Grafana 01 → bảng shard · Loki `event="shard_failed"` |
| Offline ≠ online | alert `OnlineOfflineMismatch` | `ops.feature_sync_audit.validation_report.examples` — liệt kê đúng user + feature nào lệch |
| Sync đứng giữa chừng | `lzd_feature_sync_completed_shards` < `total_shards` | `SELECT * FROM ops.feature_sync_shard WHERE status<>'DONE'` |
| Feature quá hạn | alert `FeatureStoreStale` | `ops.v_latest_sync` xem lần sync gần nhất |
| Số user tụt | alert `FeatureRowCountDrop` (so với 24h trước) | so `written_rows` với `expected_rows` trong audit |
| Redis đầy | alert `RedisMemoryHigh` | Grafana 03 → *Bộ nhớ Redis*; `noeviction` nghĩa là **ghi fail chứ không âm thầm mất key** |

### Lỗi tầng serving

| Lỗi | Báo động ở | Tìm nguyên nhân ở |
|---|---|---|
| Chưa có feature | API trả **503 kèm câu nói rõ phải chạy DAG nào** | `GET /store/info` |
| Cache miss cao | alert `FeatureCacheMissHigh` | `GET /features/{user_id}` → xem `raw_batch` rỗng hay thiếu field |
| Chậm | alert `InferenceLatencyP99High` / `FeatureLookupLatencyHigh` | 2 histogram tách riêng: tổng vs riêng Redis → biết chậm ở đâu |
| Model chưa sẵn sàng | `decision="NO_DECISION"`, `model_version="stub-0"` | `GET /store/info` → `model.is_stub` |
| Mất log inference | `lzd_inference_log_dropped_total{reason}` (`queue_full` / `db_error` / `sampled_out`) | log worker; **không bao giờ ảnh hưởng request** |
| Warm-up hỏng | log `event="warmup_failed"` kèm `step` | `/health` vẫn sống để debug, `/ready` báo đỏ |

### Lỗi tầng orchestration

| Lỗi | Báo động ở | Tìm nguyên nhân ở |
|---|---|---|
| Task fail bất kỳ | `ops.pipeline_run.status='FAILED'` + `error_message` · metric `lzd_task_failed` | log JSON có `log_url` trỏ thẳng tới task log Airflow |
| Scheduler chết | alert `AirflowSchedulerHeartbeatMissing` | Grafana 03 → *Scheduler heartbeat* |
| Task xếp hàng | `airflow_pool_open_slots` = 0 | Grafana 03 → *Pool slot* |

---

## Chưa có (biết trước để khỏi bất ngờ)

| Thiếu | Hệ quả | Bổ sung thế nào |
|---|---|---|
| **Alertmanager** | alert chỉ hiện ở Prometheus UI, không tự gửi đi đâu | thêm service `alertmanager` + `alerting:` trong `prometheus.yml` |
| **Distributed tracing** | không lần được một request xuyên qua nhiều service | OpenTelemetry + Tempo |
| **Log retention dài** | Loki giữ 7 ngày, Prometheus 15 ngày | chỉnh `retention_period` / `--storage.tsdb.retention.time` |
| **HA** | 1 broker Kafka, 1 Redis, 1 Postgres — mất là mất | ngoài phạm vi bản local |
| **TLS + secret manager** | mật khẩu nằm plaintext trong `.env` | Vault / Docker secrets khi lên thật |
| **Backup Redis/Postgres** | reset volume là mất sạch | Redis đã bật AOF; Postgres cần `pg_dump` định kỳ |

---

## Kiểm tra observability có sống không

```powershell
# 1. Prometheus đã thấy hết target chưa (phải toàn UP)
Start-Process "http://localhost:9090/targets"

# 2. Loki đã nhận log chưa
curl "http://localhost:3100/loki/api/v1/labels"

# 3. Pushgateway có metric của batch job chưa
curl "http://localhost:9091/metrics" | Select-String "lzd_feature_sync"

# 4. Grafana đã nạp datasource + dashboard chưa
curl -u admin:admin "http://localhost:3000/api/datasources"
curl -u admin:admin "http://localhost:3000/api/search?query=LZD"
```
