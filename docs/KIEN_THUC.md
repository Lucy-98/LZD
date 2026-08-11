# Kiến thức nền để hiểu hệ thống này

Tài liệu này trả lời: **hệ thống được dựng từ kiến thức nào, mỗi công nghệ giải
quyết vấn đề gì, dùng ra sao, và bẫy ở đâu.** Mỗi khái niệm đều trỏ tới đúng
file trong repo để bạn đối chiếu code chứ không học chay.

---

## 0. Nguồn kiến thức — nói rõ để bạn tự kiểm chứng

Kiến trúc này **không copy từ một tutorial nào**. Nó là ráp nối các pattern đã
thành chuẩn ngành. Trong phiên dựng hệ thống, hai thứ duy nhất được đọc trực
tiếp là ảnh `docs/img/architecture.png` và 2 trang đầu `docs/2207.09920v3.pdf`
của bạn. Phần còn lại đến từ:

| Nhóm | Nguồn gốc pattern | Xuất hiện ở đâu trong repo |
|---|---|---|
| **Feature Store** (offline/online, chống skew) | Uber Michelangelo (bài blog kỹ thuật ~2017 giới thiệu khái niệm), sau đó chuẩn hoá bởi Feast, Tecton, Databricks Feature Store | `features/spec.py`, `online_store.py`, `offline_store.py` |
| **Medallion / multi-hop** (raw → cleaned → business-ready) | Databricks đặt tên; ý tưởng cũ hơn từ mô hình staging/mart của Kimball | `dbt/models/staging/` → `dbt/models/marts/` |
| **Point-in-time correctness** | Chuẩn trong feature store và backtesting tài chính; Feast gọi là point-in-time join | `feat_user_realtime_pit.sql` |
| **At-least-once + dedup hạ nguồn** | Semantics của Kafka consumer (offset commit thủ công) | `stream_consumer.py`, `stg_app_events.sql` |
| **Blue-green / atomic pointer swap** | Pattern triển khai blue-green deployment, áp vào dữ liệu | `sync.activate_version()` |
| **Idempotency + sharding + resume** | Thực hành chuẩn của batch job phân tán | `features/sync.py` |
| **Pull-based monitoring + exporter** | Mô hình Prometheus (bắt nguồn từ Borgmon của Google) | `config/prometheus/` |
| **Log có cấu trúc, index theo label** | Mô hình Loki (label ít, không index toàn văn) | `common/logging_setup.py`, `config/promtail/` |
| **Golden signals / SLO** | *Site Reliability Engineering* (Google, sách miễn phí online) | `alerts.yml` |
| **Uplift modeling** | `docs/2207.09920v3.pdf` (DESCN, Lazada/Alibaba, KDD '22) + literature meta-learner | `training/train.py` |

Sách/tài liệu nên đọc để hiểu gốc rễ (đều là nguồn ổn định, dễ tra):

- **Designing Data-Intensive Applications** — Martin Kleppmann. Chương về
  replication, stream processing, consistency. Đây là nền của gần hết các quyết
  định trong repo này.
- **Site Reliability Engineering** — Google, đọc miễn phí tại `sre.google/books`.
  Chương *Monitoring Distributed Systems* và *Service Level Objectives*.
- Tài liệu chính thức: `kafka.apache.org/documentation`, `redis.io/docs`,
  `duckdb.org/docs`, `docs.getdbt.com`, `airflow.apache.org/docs`,
  `prometheus.io/docs`, `grafana.com/docs`.
- **Uplift**: Gutierrez & Gérardy, *Causal Inference and Uplift Modelling: A
  Review of the Literature* (2017) — bài review dễ vào nhất trước khi đọc DESCN.

> Cách dùng bảng trên: khi thắc mắc "sao lại làm thế này", tra pattern gốc chứ
> đừng tra "cách viết code X" — vấn đề nằm ở thiết kế, không ở cú pháp.

---

## 1. Bài toán gốc: uplift model là gì và vì sao nó khó

### Câu hỏi kinh doanh

Không phải *"user này có mua không?"* mà là **"phát voucher có làm user này mua
thêm không?"** — hai câu hoàn toàn khác nhau.

Chia 4 nhóm:

| | Phát voucher → mua | Phát voucher → không mua |
|---|---|---|
| **Không phát → mua** | *Sure Thing* — phát là **lỗ**, họ mua sẵn rồi | *Sleeping Dog* — phát là **phản tác dụng** |
| **Không phát → không mua** | **Persuadable** — nhóm DUY NHẤT đáng phát | *Lost Cause* — phát cũng vô ích |

Model dự đoán conversion sẽ chấm điểm cao cho *Sure Thing* → đốt tiền vào người
đã định mua. Uplift model chấm điểm cho **độ chênh**:

```
uplift(x) = P(mua | x, có voucher) − P(mua | x, không voucher)
```

### Vì sao khó: không bao giờ quan sát được cả hai

Với một user, bạn chỉ thấy **một** nhánh — đã phát thì không biết nếu không
phát họ có mua không. Đây là *fundamental problem of causal inference*. Vì thế
không có nhãn `uplift` để học trực tiếp; phải suy ra từ so sánh nhóm.

### Vì sao cần dữ liệu ngẫu nhiên

Nếu voucher chỉ phát cho user ít hoạt động (như thực tế Lazada làm), nhóm
treated và control **khác nhau ngay từ đầu** → chênh lệch quan sát được lẫn cả
"do voucher" lẫn "do hai nhóm vốn khác nhau". Paper gọi là **treatment bias**.

Dataset của bạn xử lý đúng bài này: train set có bias (giống production), test
set **randomized** để đánh giá không thiên lệch.

### Các cách tiếp cận

| Cách | Ý tưởng | Ưu / nhược |
|---|---|---|
| **T-learner** | 2 model riêng: `P(mua\|treated)`, `P(mua\|control)`, trừ nhau | Dễ nhất. Nhược: nhóm nhỏ học kém, 2 lỗi cộng dồn |
| **S-learner** | 1 model, đưa `is_treat` vào làm feature | Đơn giản. Nhược: cây quyết định dễ **bỏ qua** feature đó |
| **X-learner** | T-learner rồi impute counterfactual, học tiếp | Tốt khi treated/control lệch nhau nhiều |
| **DESCN** (paper của bạn) | Multi-task: propensity + ESTR + ESCR + pseudo treatment effect, học trên **toàn** không gian mẫu | Xử lý cả treatment bias lẫn sample imbalance. Phức tạp nhất |

### Đánh giá: AUC không dùng được

AUC đo khả năng xếp hạng *conversion*, không đo *uplift*. Dùng:

- **Qini curve / Qini coefficient** — sắp user theo uplift score giảm dần, vẽ
  độ lợi tích luỹ so với phát ngẫu nhiên.
- **AUUC** (Area Under Uplift Curve) — tương tự, chuẩn hoá khác.
- **uplift@k%** — uplift trung bình ở top k% score cao nhất. Gần nghiệp vụ nhất:
  "nếu chỉ đủ ngân sách phát 10% user, chọn theo model thì lợi bao nhiêu".

→ Chỗ để điền: `src/lzd_pipeline/training/train.py`, hàm `evaluate()`.

---

## 2. Feature Store — khái niệm trung tâm

### Vấn đề nó giải quyết

Ba vấn đề, và **cả ba đều là vấn đề tổ chức chứ không phải kỹ thuật**:

1. **Training/serving skew** — lúc train tính feature bằng SQL, lúc serve tính
   lại bằng Python. Hai công thức trôi khỏi nhau → model chạy thật kém hơn khi
   thử nghiệm, mà không ai biết vì sao.
2. **Tính lại nhiều lần** — mỗi team tự tính `gmv_30d` theo cách riêng.
3. **Không thể query kho dữ liệu lúc serve** — BigQuery mất vài giây và tính
   tiền theo lượng quét; SLA là 100ms.

### Kiến trúc hai tầng

```
        OFFLINE (nguồn sự thật)          ONLINE (bản sao phục vụ đọc)
        BigQuery / DuckDB                Redis
        ───────────────────              ─────────────────
        lịch sử đầy đủ                   chỉ giá trị MỚI NHẤT / user
        quét hàng TB                     lookup theo khoá, < 10ms
        dùng để TRAIN                    dùng để SERVE
                    └──── sync hằng ngày ────┘
```

**Điểm mấu chốt: hai bên phải cho ra cùng con số.** Toàn bộ `features/sync.py`
tồn tại chỉ để bảo đảm điều đó.

### Cơ chế chống skew trong repo này

| Cơ chế | File | Làm gì |
|---|---|---|
| **Một hợp đồng duy nhất** | `config/features/feature_spec.yml` | dbt, sync job, API đều đọc cùng file — đổi feature chỉ sửa một chỗ |
| **Hàng rào lúc build** | `dag_20` task `assert_spec_contract` | mart thiếu cột so với spec → fail ngay, không cho sync |
| **Point-in-time join** | `feat_user_realtime_pit.sql` | feature realtime lúc train tính đúng cửa sổ như lúc serve |
| **Đối chiếu sau sync** | `sync.validate_sync()` | lấy mẫu so từng giá trị Redis vs DuckDB |
| **Đối chiếu định kỳ** | `dag_50` | bắt cả trường hợp key bị sửa tay / evict sau đó |

### Point-in-time correctness — dễ sai nhất

Lúc **serve**, `rt_events_1h` = "1 giờ trước thời điểm request".
Lúc **train**, nếu tính trên toàn bộ lịch sử thì:

- phân bố khác hẳn lúc serve → skew;
- tệ hơn, nếu vô tình gộp cả event xảy ra **sau** thời điểm ra quyết định thì
  model đang nhìn trộm tương lai → **data leakage**. Offline đẹp long lanh, lên
  production sập.

Nên `feat_user_realtime_pit.sql` có điều kiện `e.event_ts < s.feature_ts`.
Dấu `<` đó là ranh giới giữa model dùng được và model tự lừa mình.

---

## 3. Kafka — hàng đợi sự kiện

### Nó là gì

**Distributed commit log**. Không phải message queue truyền thống: message
không bị xoá sau khi đọc, mà nằm lại theo retention; consumer tự nhớ mình đọc
tới đâu (offset). Nhờ đó nhiều consumer đọc cùng một luồng độc lập, và đọc lại
được khi cần.

### Khái niệm phải nắm

| Khái niệm | Nghĩa | Trong repo |
|---|---|---|
| **Topic** | Tên luồng sự kiện | `app.user.events.v1` |
| **Partition** | Topic chia nhỏ để song song. **Thứ tự chỉ bảo đảm trong 1 partition** | 3 partition |
| **Key** | Quyết định message vào partition nào | `key=user_id` → mọi event của 1 user cùng partition, đúng thứ tự |
| **Offset** | Vị trí đã đọc | commit **thủ công**, sau khi ghi xong |
| **Consumer group** | Nhóm chia nhau partition | `feature-stream-consumer` |
| **Lag** | offset mới nhất − offset đã đọc | Chỉ số sức khoẻ số 1 |
| **DLQ** | Topic chứa message hỏng | `...dlq.v1`, header `reason` |

### Ba mức bảo đảm giao hàng

| Mức | Cách làm | Hệ quả |
|---|---|---|
| At-most-once | commit offset **trước** khi xử lý | chết → **mất** dữ liệu |
| **At-least-once** | commit **sau** khi xử lý | chết → xử lý **lại** (trùng) ← repo dùng cái này |
| Exactly-once | transaction Kafka | phức tạp, chậm hơn, ràng buộc nhiều |

Chọn at-least-once vì trùng thì khử được ở hạ nguồn, còn mất thì mất luôn:

```python
# stream_consumer.py — thứ tự này là cả thiết kế
poll → validate → ghi parquet MinIO → cập nhật Redis → commit offset
                                                        └─ cuối cùng
```

Khử trùng ở `stg_app_events.sql` (`row_number() over (partition by event_id)`).

### Lệnh hay dùng

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group feature-stream-consumer          # xem lag từng partition
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic app.user.events.dlq.v1 --from-beginning --max-messages 5
```

### Bẫy

- **Lag một partition tăng, các partition khác bình thường** → dữ liệu lệch
  theo key (vài user siêu hoạt động). Không phải lỗi consumer.
- **Tăng partition không tăng được thứ tự bảo đảm** — chỉ bảo đảm trong partition.
- **Đổi schema là chuyện lớn** — nên tăng version topic (`v1` → `v2`), đừng sửa tại chỗ.

---

## 4. Redis — online store

### Nó là gì

In-memory key-value store, **xử lý lệnh đơn luồng**. Đơn luồng nghe như nhược
điểm nhưng chính là ưu điểm: mỗi lệnh chạy trọn vẹn, không cần lock.

### Cấu trúc dữ liệu dùng trong repo

| Kiểu | Dùng cho | Lệnh |
|---|---|---|
| **STRING** | con trỏ `fs:meta:active_version` | `GET` / `SET` |
| **HASH** | feature của 1 user (mỗi feature 1 field) | `HSET` / `HGETALL` / `HINCRBYFLOAT` |
| **SET** | shard đã ghi xong | `SADD` / `SISMEMBER` |
| **ZSET** | version theo timestamp, để GC | `ZADD` / `ZREVRANGE` |

### Bốn kỹ thuật cần hiểu

**1. Pipeline ≠ Transaction.** Pipeline gộp nhiều lệnh vào một lần gửi để đỡ
round-trip, nhưng lệnh của client khác **vẫn chen vào giữa được**. Muốn nguyên
khối phải dùng `MULTI/EXEC` hoặc Lua.

**2. Lua = atomic thật.** Script chạy trọn vẹn, không gì chen vào. Repo dùng ở
`read_for_serving()` (đọc version + 2 hash trong 1 lần) và
`incr_realtime_counters()` (cộng ô + prune ô cũ).

**3. `SCAN` chứ tuyệt đối không `KEYS`.** `KEYS *` khoá server tới khi quét
xong — trên production là sự cố. `SCAN` trả về từng mẻ theo con trỏ.

```python
# online_store.count_keys() / expire_version() — luôn dùng SCAN
cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=1000)
```

**4. `UNLINK` thay `DEL`** khi xoá nhiều key — xoá bất đồng bộ, không chặn.

### Cửa sổ trượt: chỗ dễ sai nhất

Cách ngây thơ — `HINCRBY` một field rồi `EXPIRE` lại key sau mỗi lần ghi —
**sai**: user hoạt động liên tục thì TTL bị đẩy lùi mãi, key không bao giờ hết
hạn, counter cộng dồn cả ngày. Cách đúng: chia giờ thành **12 ô 5 phút**, mỗi ô
một field `rt_events_1h|<mốc ô>`; đọc = cộng ô còn trong cửa sổ, ghi = cộng ô
hiện tại + xoá ô đã rớt ra.

### `maxmemory-policy` — quyết định có chủ đích

Repo đặt `noeviction`, **không** phải `allkeys-lru`. Vì với LRU, Redis sẽ âm
thầm xoá feature của user ít truy cập → API đọc miss → model quyết định bằng
toàn giá trị default mà **không ai biết**. Thà ghi fail và báo động.

### Lệnh hay dùng

```bash
docker compose exec redis redis-cli GET fs:meta:active_version
docker compose exec redis redis-cli HGETALL fs:v20260805:u:U0000123
docker compose exec redis redis-cli --scan --pattern "fs:v20260805:u:*" | wc -l
docker compose exec redis redis-cli INFO memory
docker compose exec redis redis-cli --latency        # đo độ trễ thật
```

---

## 5. DuckDB — kho dữ liệu (đóng vai BigQuery)

### Nó là gì

**"SQLite cho phân tích"**: OLAP nhúng, chạy trong process, lưu theo cột,
vectorized. Không server, không cluster.

### Vì sao dùng để mô phỏng BigQuery

| | BigQuery | DuckDB |
|---|---|---|
| Cột (columnar) | ✓ | ✓ |
| Đọc parquet trên object storage | GCS | S3/MinIO qua `httpfs` |
| SQL phân tích | ✓ | ✓ (cú pháp rất gần) |
| Quy mô | petabyte, phân tán | một máy |
| Chi phí | theo lượng quét | 0 |

Logic SQL học ở DuckDB chuyển sang BigQuery gần như nguyên vẹn.

### Giới hạn quan trọng nhất: single-writer

Một process mở file ở chế độ ghi là **khoá cả file**. Repo xử lý bằng hai lớp:

1. Airflow pool `duckdb_writer` = **1 slot** → không bao giờ 2 task cùng ghi.
2. `duckdb_conn()` mặc định **read-only**; muốn ghi phải gọi `duckdb_writer()`
   cho lộ ý định ra. Không có lớp 2 thì chỉ cần một notebook gõ `duckdb_conn()`
   là dbt lúc 01:00 chết giữa chừng.

### Cú pháp DuckDB đáng biết (repo dùng)

```sql
SELECT * EXCLUDE (data_id, label)      -- lấy hết trừ vài cột
FROM read_parquet('s3://bucket/**/*.parquet');

COPY (SELECT ...) TO 's3://...' (FORMAT PARQUET, COMPRESSION SNAPPY);

SELECT * FROM t QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts) = 1;

SELECT count(*) FILTER (WHERE event_type = 'order') FROM events;
```

`QUALIFY` lọc trên kết quả window function mà không cần subquery — dùng để khử
trùng. `EXCLUDE` cực tiện khi có 83 cột feature.

---

## 6. dbt — biến đổi dữ liệu bằng SQL có kỷ luật

### Nó là gì

dbt **không** di chuyển dữ liệu. Nó biên dịch SQL có tham số thành SQL thật, chạy
trong kho, và quản lý: thứ tự phụ thuộc, materialization, test, tài liệu.

### Ý tưởng cốt lõi: `ref()` sinh ra DAG

```sql
select * from {{ ref('stg_app_events') }}
```

Từ các `ref()`, dbt **tự suy ra** thứ tự chạy. Bạn không bao giờ phải tự sắp xếp.

### Materialization

| Kiểu | Sinh ra | Dùng khi |
|---|---|---|
| `view` | VIEW | staging — nhẹ, luôn tươi |
| `table` | TABLE (drop + tạo lại) | marts — đọc nhiều |
| `incremental` | chỉ thêm dòng mới | bảng lớn (repo chưa dùng, là bước nâng cấp tiếp theo) |
| `ephemeral` | CTE nội tuyến | logic trung gian |

### Medallion — vì sao chia 3 tầng

```
raw       giữ NGUYÊN như nguồn, không sửa gì → còn đối chứng khi nghi ngờ
staging   ép kiểu, đổi tên, khử trùng      → KHÔNG logic nghiệp vụ
marts     logic nghiệp vụ, sẵn sàng dùng    → cái mà consumer đọc
```

Lợi ích thật: khi số liệu sai, bạn lần ngược từng tầng để biết nó hỏng ở đâu.
Nếu nhét hết vào một query 500 dòng thì chỉ còn cách đọc lại từ đầu.

### Test

```yaml
columns:
  - name: event_id
    tests: [not_null, unique]      # unique chính là bằng chứng dedup chạy đúng
```

`store_failures: true` (trong `dbt_project.yml`) ghi **các dòng vi phạm** vào
schema `dq_failures` → khi test fail bạn xem được đúng dòng nào hỏng:

```sql
SELECT * FROM dq_failures.<tên_test> LIMIT 20;
```

### Lệnh

```bash
dbt debug                      # kiểm tra kết nối
dbt run --vars '{"run_date": "2026-08-05"}'
dbt run --select feat_user_serving+     # model đó và mọi thứ phụ thuộc nó
dbt test --select stg_app_events
dbt docs generate && dbt docs serve     # sơ đồ phụ thuộc tương tác
```

---

## 7. Airflow — điều phối

### Nó là gì

Lập lịch và chạy các **DAG** (đồ thị task có hướng, không chu trình). Vai trò:
quyết định *cái gì chạy, khi nào, sau cái gì, thất bại thì làm sao* — **không
phải** nơi xử lý dữ liệu.

> Nguyên tắc trong repo: DAG chỉ là lớp điều phối mỏng, mọi logic nằm trong
> `src/lzd_pipeline/`. Nhờ đó test được bằng `pytest` mà không cần Airflow.

### Khái niệm

| Khái niệm | Nghĩa |
|---|---|
| **logical_date** | Ngày **dữ liệu** thuộc về, không phải lúc chạy. Nền tảng của backfill và idempotency |
| **catchup** | Có chạy bù các kỳ đã lỡ không |
| **Pool** | Giới hạn số task song song dùng chung tài nguyên (`duckdb_writer` = 1) |
| **Trigger rule** | Điều kiện chạy: `all_success` (mặc định), `all_done`, `one_failed` |
| **XCom** | Truyền giá trị **nhỏ** giữa task. Không phải để chuyển dữ liệu |
| **Dynamic task mapping** | `.expand()` sinh N task lúc chạy — repo dùng cho 32 shard |
| **Executor** | Local (repo) / Celery / Kubernetes |

### Ba pattern trong repo

**1. Dynamic task mapping** — 32 shard, retry riêng từng cái:
```python
sync_shard.expand(shard_id=list(range(SHARDS)))
```

**2. Trigger rule để lộ lỗi** — task này chạy *cả khi* dbt test fail, vì đó
chính là lúc cần nhìn số liệu nhất:
```python
@task(trigger_rule=TriggerRule.ALL_DONE)
def publish_dbt_results(...)
```

**3. Callback ghi sổ** — `lzd_utils/callbacks.py` gắn vào mọi task: ghi
`ops.pipeline_run`, đẩy metric, log JSON có `log_url` trỏ thẳng tới task log.

### Bẫy

- **Code cấp module chạy mỗi lần parse DAG** (vài giây/lần). Import nặng phải
  đặt **trong** hàm task — repo làm đúng vậy.
- **XCom không phải để chuyển dữ liệu** — chỉ metadata nhỏ.
- **Retry chỉ an toàn khi task idempotent.** Đây là lý do `sync_shard` được
  thiết kế để chạy lại vô hại.

---

## 8. Observability — Prometheus, Grafana, Loki

### Vì sao cần cả ba

Grafana **không thu thập và không lưu** gì cả — nó chỉ vẽ. Phải có người sinh
số liệu (exporter / service), người lưu (Prometheus cho metric, Loki cho log).

```
service + exporter ──► Prometheus ──┐
                                     ├──► Grafana
container logs ──► Promtail ──► Loki ┘
```

### Prometheus — mô hình kéo (pull)

Prometheus **tự đi hỏi** `/metrics` mỗi 15 giây. Ngược với mô hình đẩy.

| Loại metric | Nghĩa | Ví dụ |
|---|---|---|
| **Counter** | Chỉ tăng | `lzd_events_consumed_total` |
| **Gauge** | Lên xuống | `lzd_feature_store_age_seconds` |
| **Histogram** | Phân bố theo khoảng | `lzd_inference_latency_seconds` |

Counter luôn dùng kèm `rate()` — giá trị tuyệt đối vô nghĩa:

```promql
sum(rate(lzd_events_consumed_total[1m]))                      # sự kiện/giây
histogram_quantile(0.99,
  sum by (le) (rate(lzd_inference_latency_seconds_bucket[5m])))  # p99
```

**Vì sao cần Pushgateway.** Task Airflow sống 30 giây rồi chết — Prometheus
scrape 15s/lần sẽ hụt. Nên batch job chủ động **đẩy** metric ra, Pushgateway
giữ hộ tới lần scrape sau. Chỉ dùng cho batch job; service chạy dài thì cứ để
Prometheus kéo.

### Loki — "Prometheus cho log"

Điểm khác biệt: **chỉ index label, không index toàn văn**. Rẻ hơn Elasticsearch
rất nhiều, đổi lại phải lọc theo label trước rồi mới grep trong đó.

```logql
{component="stream-consumer"} | json | level="ERROR"
{job="docker"} | json | event="shard_written"
{job="airflow-tasks", dag_id="40_sync_features_to_redis"}
```

→ Đây là lý do mọi service trong repo log **JSON** với trường `event` cố định:
để lọc theo `event="shard_failed"` thay vì đoán câu chữ.

### Bốn tín hiệu vàng (Golden Signals)

Từ sách SRE của Google — luôn hỏi 4 câu này cho mọi service:

| Tín hiệu | Trong repo |
|---|---|
| **Latency** | `lzd_inference_latency_seconds`, `lzd_feature_lookup_seconds` |
| **Traffic** | `lzd_inference_requests_total`, `lzd_events_consumed_total` |
| **Errors** | `lzd_events_dlq_total`, `lzd_feature_sync_status` |
| **Saturation** | `redis_memory_used_bytes`, `kafka_consumergroup_lag` |

---

## 9. MinIO & MLflow

### MinIO = S3 chạy local

API tương thích S3 hoàn toàn, nên `boto3` và DuckDB `httpfs` dùng y như thật.
Lên production chỉ đổi endpoint. Trong repo là **data lake**: parquet thô.

**Vì sao Parquet chứ không CSV**: lưu theo cột (đọc 3/83 cột thì chỉ tốn 3),
có schema kèm theo, nén tốt hơn nhiều, và mọi engine phân tích đều đọc được.

### MLflow

| Thành phần | Việc |
|---|---|
| **Tracking** | Ghi param, metric, artifact của từng lần train |
| **Model Registry** | Quản lý phiên bản model + alias (Staging/Production) |
| **Artifact store** | Nơi để file model — ở đây là MinIO |

Chi tiết quan trọng trong repo: lúc train ghi lại `feature_list.json` **đúng
thứ tự cột**. Lúc serve nạp lại đúng danh sách đó. Không có bước này, chỉ cần
thứ tự cột đổi là model đọc sai feature mà **không báo lỗi gì**.

---

## 10. Bảy nguyên tắc thiết kế — phần đáng học nhất

Công nghệ thay đổi, mấy nguyên tắc này thì không.

### 1. Idempotency — chạy lại cho cùng kết quả

```python
def make_version(logical_date) -> str:
    return f"v{logical_date.strftime('%Y%m%d')}"   # 2026-08-05 → LUÔN là v20260805
```

Version quyết định theo ngày logic (không phải `now()`, không phải UUID) → chạy
lại ghi đè đúng chỗ cũ. Cộng với `HSET` (ghi đè, không cộng dồn) và đánh dấu
shard đã xong → chạy lại bao nhiêu lần cũng ra một kết quả.

**Câu hỏi tự kiểm:** *"Chạy lại job này 3 lần thì dữ liệu có nhân 3 không?"*
Nếu chưa trả lời chắc chắn được thì job chưa xong.

### 2. Nguồn sự thật duy nhất

DuckDB là sự thật, Redis là bản sao. Khi lệch → **luôn tin DuckDB**. Có quy tắc
này thì mọi tranh cãi "số nào đúng" đều kết thúc trong 5 giây.

### 3. Đổi nguyên tử, không đổi từ từ

Ghi vào namespace mới rồi đổi **một** con trỏ:

```python
SET fs:meta:active_version v20260805     # 1 lệnh — hoặc toàn bộ cũ, hoặc toàn bộ mới
```

Không bao giờ có trạng thái "một nửa user dùng feature mới". Rollback = trỏ
ngược lại, dưới 5 giây, không phải ghi lại byte nào.

### 4. Kiểm tra trước khi công bố

```
ghi → validate → (đạt?) → activate
              └→ (hỏng?) → dừng, giữ nguyên bản cũ
```

Sync hỏng giữa chừng **không ảnh hưởng người dùng** vì con trỏ chưa hề đổi.
Đây là lý do bạn có thể ngủ yên khi DAG fail lúc 1h30 sáng.

### 5. Chia nhỏ để hỏng có giới hạn

32 shard độc lập → hỏng 3 shard thì retry 3 shard, không phải làm lại từ đầu.
Nguyên tắc chung: **đơn vị công việc càng nhỏ, chi phí một lần hỏng càng nhỏ**.

### 6. Fail nhanh, fail rõ

```python
raise ValueError(
    f"Mart không khớp feature_spec.yml, thiếu: {missing[:15]}\n"
    "Sửa dbt model hoặc sửa feature_spec.yml rồi chạy lại."
)
```

Thông báo lỗi nói **cái gì sai** và **làm gì tiếp**. Lỗi im lặng đắt hơn lỗi ồn
ào rất nhiều — nhất là trong ML, nơi sai sót không làm sập gì cả, chỉ làm model
tệ đi âm thầm.

### 7. Mặc định phải an toàn

`duckdb_conn()` mặc định read-only. Muốn nguy hiểm thì phải gõ thêm chữ. Thiết
kế API sao cho **cách dùng dễ nhất cũng là cách dùng đúng nhất**.

---

## 11. Lộ trình học đề nghị

### Tuần 1 — chạy được và nhìn thấy

1. `.\scripts\stack.ps1 init` rồi `up-all`
2. Mở lần lượt: Airflow → Kafka UI → MinIO → Grafana → RedisInsight
3. Chạy DAG `00` (đặt `row_limit=200000`) → `20` → `40`
4. `curl localhost:8000/features/U0000123` — nhìn tận mắt cái model sẽ nhận

**Tự kiểm:** vẽ lại đường đi của một event từ producer tới `/decide`, gọi tên
đúng từng thành phần nó đi qua.

### Tuần 2 — hiểu vì sao

1. Đọc [DATA_FLOW.md](DATA_FLOW.md) song song với code
2. Làm hết 6 bài diễn tập trong [RUNBOOK.md](RUNBOOK.md)
3. Đọc `features/sync.py` và `features/online_store.py` từ đầu tới cuối

**Tự kiểm:** giải thích được vì sao `active_version` tồn tại, mà không nhìn tài liệu.

### Tuần 3 — tự phá và tự sửa

1. `99_ops_toolbox` → `chaos_partial_fail` → chạy lại DAG 40
2. Bỏ một cột trong `feat_user_serving.sql` → xem hàng rào bắt được không
3. `chaos_flush_realtime` → theo dõi cache miss trên Grafana rồi tự phục hồi
4. Sửa `PRODUCER_CORRUPT_RATE=0.2` → xem DLQ và alert

**Tự kiểm:** một alert nổ, bạn mất bao lâu để chỉ ra dòng dữ liệu gây ra nó?

### Tuần 4 — mở rộng

Chọn một:
- Thêm feature mới → sửa spec → sửa dbt → sync → thấy nó ở `/features/{id}`
- Đổi `feat_user_serving` sang `incremental` (bảng lớn thì rebuild cả bảng là phí)
- Thêm Alertmanager để alert gửi được đi đâu đó
- Điền phần model: `train.py` → `model_loader.py`

---

## 12. Tự kiểm tra hiểu bài

Trả lời được hết là nắm chắc hệ thống:

**Feature store**
1. Vì sao không query thẳng DuckDB lúc serve?
2. Nếu bỏ `active_version`, ghi thẳng đè lên key đang dùng thì hỏng chuyện gì?
3. Sync chết ở shard 17/32 thì người dùng có bị ảnh hưởng không? Vì sao?

**Chống skew**
4. Ba cơ chế nào trong repo cùng nhau chống training/serving skew?
5. Vì sao `feat_user_realtime_pit.sql` có `e.event_ts < s.feature_ts`? Bỏ dấu `<` thì sao?
6. Vì sao `rt_events_1h` phải chia ô 5 phút thay vì một counter + TTL?

**Streaming**
7. Consumer commit offset **sau** khi ghi — được gì, mất gì?
8. Dữ liệu trùng do at-least-once được khử ở đâu?
9. Vì sao dùng `user_id` làm key Kafka?

**Vận hành**
10. Chạy lại DAG 40 hai lần thì số user trên Redis có gấp đôi không? Vì sao?
11. Redis `noeviction` thay vì `allkeys-lru` — lý do?
12. Feature sai đã lên production rồi, khôi phục thế nào và mất bao lâu?

Gợi ý: câu 3, 6, 10, 12 nằm ở `features/sync.py` và `features/online_store.py`.

---

## 13. Đọc thêm theo chủ đề

| Muốn hiểu sâu | Đọc |
|---|---|
| Hệ phân tán, consistency, stream | *Designing Data-Intensive Applications* — Kleppmann |
| Monitoring, SLO, on-call | *Site Reliability Engineering* — Google (`sre.google/books`) |
| Feature store thực chiến | Tài liệu Feast (`docs.feast.dev`) — khái niệm entity, feature view, point-in-time join |
| Kafka | *Kafka: The Definitive Guide* (O'Reilly), hoặc `kafka.apache.org/documentation` |
| dbt | `docs.getdbt.com` — mục *Best practices* |
| Uplift / causal inference | Gutierrez & Gérardy (2017) → rồi `docs/2207.09920v3.pdf` |
| Nợ kỹ thuật trong ML | *Hidden Technical Debt in Machine Learning Systems* — Sculley et al., NeurIPS 2015 |

> Bài cuối cùng đáng đọc nhất nếu bạn định theo ML engineering: nó chỉ ra rằng
> code model chỉ là một ô nhỏ giữa một hệ thống khổng lồ toàn hạ tầng dữ liệu —
> đúng bằng tỉ lệ trong repo này (`train.py` là file để trống, mọi thứ còn lại
> thì đã đầy).
