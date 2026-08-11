# Runbook — xử lý sự cố & diễn tập

## Bảng tra nhanh

| Triệu chứng | Nhìn ở đâu trước | Nguyên nhân hay gặp | Xử lý |
|---|---|---|---|
| API trả 503 "chưa có active_version" | `curl :8000/store/info` | chưa chạy DAG 40 lần nào | chạy DAG 20 → 40 |
| Cache miss cao | Grafana 00 → *Cache hit rate* | user không có trong snapshot, hoặc vừa GC nhầm | so `ops.feature_sync_audit.written_rows` với row count mart |
| Alert `FeatureStoreStale` | Grafana 01 → *Tuổi feature online* | DAG 40 fail hoặc bị pause | xem `ops.feature_sync_shard` |
| Alert `OnlineOfflineMismatch` | Grafana 01 → *Báo cáo validate* | dbt build lại giữa lúc sync | rollback + resync |
| Kafka lag tăng đều | Grafana 02 → *Lag theo partition* | consumer chết / chậm | `docker compose logs stream-consumer` |
| DLQ tăng | Kafka UI → topic `.dlq.v1` → header `reason` | producer đổi schema | sửa producer hoặc `validate_event` |
| DAG 20 fail ở `assert_spec_contract` | task log | mart lệch `feature_spec.yml` | sửa dbt model **hoặc** spec, rồi chạy lại |
| DuckDB "Could not set lock" | task log | 2 task cùng ghi | kiểm tra pool `duckdb_writer` phải = 1 slot |

---

## SC-1 · Sync chết giữa chừng

**Triệu chứng:** DAG 40 fail ở một vài task `sync_shard`.

**Tình trạng thực tế:** `active_version` **chưa hề đổi** → serving vẫn chạy bản
cũ bình thường. Không có sự cố với người dùng. Không cần vội.

```sql
-- shard nào hỏng
SELECT shard_id, status, rows_written, attempt, error_message
FROM ops.feature_sync_shard
WHERE feature_version = 'v20260805' AND status <> 'DONE'
ORDER BY shard_id;
```

**Xử lý:** chạy lại DAG 40 (hoặc *Clear* các task fail). Shard đã `DONE` bị bỏ
qua nhờ `fs:meta:{v}:shards`, chỉ shard hỏng được ghi lại.

Cách khác — chỉ vá shard thiếu, không đụng DAG:
`99_ops_toolbox` → `action=resync_pending`, `target_date=2026-08-05`.

---

## SC-2 · Feature sai sau khi đã activate

**Triệu chứng:** alert `OnlineOfflineMismatch`, hoặc business báo voucher phát sai.

**Xử lý tức thì (< 5 giây):** `99_ops_toolbox` → `action=rollback`
→ `SET fs:meta:active_version` về bản trước. Không phải ghi lại dữ liệu vì
version cũ vẫn còn nguyên trong Redis (đây là lý do giữ 2 version).

```powershell
docker compose exec redis redis-cli GET fs:meta:active_version
```

**Sau đó điều tra:**
```sql
SELECT feature_version, status, checksum, validation_report
FROM ops.feature_sync_audit ORDER BY started_at DESC LIMIT 5;
```
`validation_report.examples` liệt kê user + feature bị lệch.

---

## SC-3 · Streaming đứt

**Triệu chứng:** `NoEventsIngested`, overlay `rt:u:*` hết hạn dần.

**Ảnh hưởng:** feature realtime rơi về default; feature batch **vẫn còn** →
model vẫn quyết định được, chỉ kém nhạy. Đây là chủ đích thiết kế: overlay chỉ
là lớp phụ, mất nó không sập serving.

```powershell
docker compose logs --tail=200 stream-consumer
docker compose restart stream-consumer
```
Consumer đọc lại từ offset đã commit → không mất dữ liệu.

---

## SC-4 · Redis đầy bộ nhớ

`maxmemory-policy` đang là `noeviction` — **cố ý**. Nếu để `allkeys-lru`, Redis
sẽ tự xoá feature của user ít truy cập → cache miss âm thầm, model quyết định
bằng toàn giá trị default mà không ai biết. Thà ghi fail và báo động.

```powershell
docker compose exec redis redis-cli INFO memory
```
Xử lý: giảm `FEATURE_VERSIONS_TO_KEEP` → chạy `99_ops_toolbox` `action=gc_versions`,
hoặc tăng `REDIS_MAXMEMORY` trong `.env`.

---

# Bài diễn tập cho intern

## Bài 1 · Chứng minh idempotency

```
1. Chạy DAG 40 tới khi xanh hết
2. Ghi lại: SELECT written_rows FROM ops.feature_sync_audit WHERE feature_version='v...'
3. Trigger DAG 40 lần nữa (cùng ngày logic)
4. Xem log:  {job="docker"} | json | event="shard_skipped"
```
**Kỳ vọng:** lần 2 xong trong vài giây, `written_rows` **không tăng**, mọi shard
đều `skipped`. Đó là idempotency — rerun không double-write.

## Bài 2 · Phục hồi khi hỏng một phần

```
1. 99_ops_toolbox → action=chaos_partial_fail, fail_shards=5
2. Xem Grafana 01 → bảng shard: 5 dòng chuyển FAILED
3. Chạy lại DAG 40
4. Xem log: chỉ 5 shard đó có event="shard_written", 27 shard còn lại "shard_skipped"
```

## Bài 3 · Atomic swap

```
1. Mở 2 cửa sổ:
     A: while($true){ curl -s localhost:8000/features/U0000123 | ConvertFrom-Json | % feature_version; sleep 1 }
     B: chạy DAG 40
2. Quan sát cửa sổ A
```
**Kỳ vọng:** feature_version nhảy từ `v...04` sang `v...05` **một phát**, không
có khoảnh khắc nào trả về dữ liệu nửa cũ nửa mới.

## Bài 4 · Nhìn thấy training/serving skew

```
1. Sửa dbt/models/marts/feat_user_serving.sql — bỏ dòng `s.f42,`
2. Chạy DAG 20
```
**Kỳ vọng:** task `assert_spec_contract` **fail** với thông báo thiếu `f42`.
Đó là hàng rào — nếu không có, `f42` sẽ âm thầm rơi về default lúc serve trong
khi model đã học trên giá trị thật.

## Bài 5 · Đo SLA thật

```powershell
python scripts/load_test.py --rps 50 --duration 120
```
Đối chiếu p99 in ra với Grafana 00 → *p99 Redis feature lookup* (mục tiêu <10ms)
và *p99 decision latency* (SLA <100ms).

## Bài 6 · Mất overlay realtime

```
1. 99_ops_toolbox → action=chaos_flush_realtime
2. curl localhost:8000/features/U0000123 → rt_* đều = 0 (default)
3. Đợi 1-2 phút → gọi lại → rt_* có giá trị trở lại
```
Hiểu: overlay tự phục hồi từ stream, còn feature batch thì không — nó phụ thuộc
job hằng ngày.

---

## Lệnh hay dùng

```powershell
# Redis
docker compose exec redis redis-cli GET fs:meta:active_version
docker compose exec redis redis-cli SMEMBERS fs:meta:v20260805:shards
docker compose exec redis redis-cli HGETALL fs:v20260805:u:U0000123
docker compose exec redis redis-cli HGETALL rt:u:U0000123
docker compose exec redis redis-cli --scan --pattern "fs:v20260805:u:*" | Measure-Object -Line

# DuckDB
docker compose exec airflow-scheduler python -c "from lzd_pipeline.common.clients import duckdb_conn; con=duckdb_conn(read_only=True).__enter__(); print(con.execute('SELECT COUNT(*) FROM marts.feat_user_serving').fetchall())"

# Postgres audit
docker compose exec postgres psql -U lzd -d pipeline -c "SELECT * FROM ops.v_latest_sync LIMIT 5;"
docker compose exec postgres psql -U lzd -d pipeline -c "SELECT * FROM ops.v_dq_last_24h;"

# Kafka
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group feature-stream-consumer
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic app.user.events.dlq.v1 --from-beginning --max-messages 5

# Airflow
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow tasks test 40_sync_features_to_redis prepare 2026-08-05
```
