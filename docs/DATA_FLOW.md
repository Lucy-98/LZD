# DATA FLOW ARCHITECTURE

> **Kiến trúc luồng dữ liệu**: Tích hợp luồng Batch (Track A + dbt Medallion 3 tầng) và Near-Realtime (Track B + Kafka + Redis Realtime Overlay + Hybrid Trigger Engine).

---

## 1. Luồng Batch (Offline Feature Pipeline)

```text
Dataset CSV (full_trainset.csv)
       │
       │ DAG 60: Track A Solver (Constraint Reconstruction)
       ▼
┌─────────────────────────────────┬──────────────────────────────────┐
│ MinIO: raw/events_v2/*.parquet  │ DuckDB: schema biz.*             │
│ (Chuỗi Raw Events E*)           │ • customer_attribute (T2)        │
│                                 │ • passthrough_source (T3)        │
│                                 │ • encoding_map (T2 decode)       │
│                                 │ • reconstruction_boundary        │
└────────────────┬────────────────┴────────────────┬─────────────────┘
                 │                                 │
                 ▼                                 ▼
 🥉 TẦNG ĐỒNG: stg_events_v2                      biz.*
                 │                                 │
                 ▼                                 ▼
 🥈 TẦNG BẠC: int_cfs_counter, int_cfs_recency, int_cfs_categorical, int_passthrough
                 │                                 │
                 ├─────────────────────────────────┘
                 ▼
 🥇 TẦNG VÀNG:
   ├── marts.training_features (30 cột f* phục vụ Jupyter Notebook huấn luyện mô hình)
   └── marts.serving_features  (~41 cột tên nghiệp vụ: customer_value_score, order_cnt_7d...)
                 │
                 │ DAG 40: Đồng bộ 32 Shards (01:30 AM)
                 ▼
          Redis Online Feature Store: fs:{version}:u:{user_id}
```

---

## 2. Luồng Near-Realtime (Online Streaming & In-Session Intent)

```text
Hành vi Người dùng Sau t₀ (browse, add_to_cart, search, checkout)
       │
       ▼
 Kafka topic: app.user.events.v1
       │
       ▼
 Stream Consumer (poll 1s, flush 30s hoặc 2000 events)
       │
       ├─► [1] MinIO raw/app_events/*.parquet (Immutable Lakehouse)
       │
       └─► [2] Redis Realtime Overlay: rt:u:{user_id} (TTL 48 giờ)
               • Cửa sổ 5 phút: rt_page_view_5m, rt_add_to_cart_5m, rt_cart_gmv_5m, rt_search_cnt_5m
               • Cửa sổ 1 giờ: rt_events_1h, rt_page_view_1h, rt_add_to_cart_1h, rt_order_1h...
```

---

## 3. Luồng Serving & Hybrid Trigger Engine

```text
 ① Event-Driven Trigger (sự kiện add_to_cart, checkout) ──┐
 ② Polling Trigger (Quét định kỳ mỗi 5 phút user active) ──┴─► Trigger Engine
                                                                     │
                                    ┌────────────────────────────────┘
                                    ▼
                          Đọc 1 RTT Atomic Lua Script:
                          • Batch: fs:{version}:u:{uid} (~41 features)
                          • Realtime: rt:u:{uid} (11 features 5m/1h)
                                    │
                                    ▼
                          Model Inference Engine (Best Model từ Notebook)
                          Dự đoán Uplift Score τ(x)
                                    │
                                    ▼
                          Quyết định phân phối Voucher:
                          • τ(x) ≥ 0.05 ──► voucher_30 (giảm 30%)
                          • τ(x) ≥ 0.02 ──► voucher_15 (giảm 15%)
                          • τ(x) < 0.02 ──► no_voucher (tiết kiệm ngân sách)
```
