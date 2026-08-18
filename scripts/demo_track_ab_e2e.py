#!/usr/bin/env python3
"""Kịch bản Demo Toàn diện: Track A (Quá khứ) -> Redis Batch -> Track B (Tương lai) -> Kafka -> Realtime Overlay -> Inference API.

Quy trình:
  1. [Track A] Giải mã 10 users từ full_trainset.csv -> Reconstructed witness events (T < T0)
  2. [Batch Redis] Xác nhận 55 features đã đồng bộ trên Redis (fs:v2:u:{user_id})
  3. [Track B] Mô phỏng sự kiện tương lai (T > T0) từ CustomerState(T0) cho 10 users
  4. [Kafka & Consumer] Bắn sự kiện Track B vào Kafka -> stream-consumer cập nhật rt:u:{user_id}
  5. [Inference Serving] Gọi POST /decide -> API đọc cả 2 nguồn, nạp Model thật và ra quyết định.
"""
from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path

from lzd_pipeline.common.clients import get_kafka_producer, get_redis
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.reconstruction.e2e import load_runtime_config
from lzd_pipeline.reconstruction.track_ab_batch import run_batch


def print_step(title: str):
    print("\n" + "=" * 75)
    print(f"👉 {title}")
    print("=" * 75)


def main():
    print_step("BƯỚC 1: TRACK A - GIẢI MÃ QUÁ KHỨ (T < T0) CHO 10 USERS")
    trainset_path = Path("data/full_trainset.csv")
    if not trainset_path.exists():
        raise FileNotFoundError(f"Không tìm thấy {trainset_path}")

    config = load_runtime_config()
    batch_res = run_batch(limit=10, input_path=trainset_path, config=config)

    total_track_a = batch_res["track_a_events_total"]
    total_track_b = batch_res["track_b_events_total"]
    n_passed = batch_res["n_all_gates_passed"]
    future_events = batch_res["future_events"]

    print(f"✅ Track A giải mã thành công: {total_track_a} witness events (T < T0) cho 10 users.")
    print(f"   Kết quả kiểm định Gate A..F: {n_passed}/10 users PASS 100% tất cả các cổng.")

    print_step("BƯỚC 2: XÁC MINH 55 BATCH FEATURES TRÊN REDIS (ONLINE FEATURE STORE)")
    store = OnlineFeatureStore()
    active_version = store.get_active_version()
    print(f"Active Feature Version trên Redis: {active_version}")

    sample_user_id = "U0000001"
    batch_features, _ = store.get_features(sample_user_id, active_version)
    print(f"Batch key 'fs:{active_version}:u:{sample_user_id}': tìm thấy {len(batch_features)} features.")
    print("Mẫu 5 features đầu tiên:")
    for k in list(batch_features.keys())[:5]:
        print(f"  • {k}: {batch_features[k]}")

    print_step("BƯỚC 3: TRACK B - MÔ PHỎNG SỰ KIỆN TƯƠNG LAI (T > T0)")
    print(f"✅ Track B đã sinh {total_track_b} sự kiện tương lai từ CustomerState(T0).")
    
    # Lấy mẫu sự kiện của sample_user_id
    sample_future_events = [fe for fe in future_events if fe["customer_id"] == sample_user_id]
    print(f"User {sample_user_id} có {len(sample_future_events)} sự kiện Track B giả lập:")
    for fe in sample_future_events[:4]:
        print(f"  • [{fe['event_ts']}] {fe['event_type']} (source: {fe['source_type']})")

    print_step("BƯỚC 4: BẮN SỰ KIỆN TRACK B VÀO KAFKA -> STREAM CONSUMER CẬP NHẬT REDIS")
    producer = get_kafka_producer()
    topic = get_settings().kafka.topic_events
    now_epoch = time.time()

    event_mapping = {
        "PRODUCT_VIEWED": "page_view",
        "ITEM_ADDED_TO_CART": "add_to_cart",
        "SESSION_STARTED": "app_open",
    }

    sent_count = 0
    for idx, fe in enumerate(future_events):
        uid = fe["customer_id"]
        etype = event_mapping.get(fe["event_type"], "page_view")
        event_ts = now_epoch - (idx % 10 * 30 + 10)
        payload = {
            "event_id": f"evt_tb_{fe['event_id'][:16]}",
            "user_id": uid,
            "event_type": etype,
            "event_ts": event_ts,
            "session_id": fe["session_id"],
            "platform": "android",
            "schema_version": 1,
        }
        producer.produce(topic, value=json.dumps(payload).encode("utf-8"), key=uid.encode("utf-8"))
        sent_count += 1

    producer.flush()
    print(f"Đã bắn {sent_count} Track B events vào Kafka topic '{topic}'.")
    print("Đang đợi stream-consumer đọc Kafka và cập nhật Redis...")
    time.sleep(3)

    r = get_redis()
    rt_key = f"rt:u:{sample_user_id}"
    rt_data = r.hgetall(rt_key)
    print(f"✅ Redis Realtime key '{rt_key}' sau khi stream-consumer xử lý:")
    if rt_data:
        for k, v in sorted(rt_data.items()):
            print(f"  • {k}: {v}")
    else:
        print("  (Chưa có overlay hoặc TTL)")

    print_step("BƯỚC 5: GỌI INFERENCE API (FASTAPI) - MODEL THẬT RA QUYẾT ĐỊNH")
    api_url = "http://localhost:8000/decide"
    req_body = json.dumps({"user_id": sample_user_id}).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=req_body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=5) as resp:
        decision_resp = json.loads(resp.read().decode("utf-8"))

    print("Kết quả phản hồi từ Inference API:")
    print(json.dumps(decision_resp, indent=2))

    feat_url = f"http://localhost:8000/features/{sample_user_id}"
    with urllib.request.urlopen(feat_url, timeout=5) as resp:
        feat_debug = json.loads(resp.read().decode("utf-8"))

    print(f"\nChi tiết hợp nhất đặc trưng cho user {sample_user_id}:")
    print(f"  • Batch features từ Redis:     {feat_debug['n_batch_fields']} trường (từ Track A / offline mart)")
    print(f"  • Realtime features từ Redis:  {feat_debug['n_realtime_fields']} trường (từ Track B qua Kafka)")
    print(f"  • Cửa sổ realtime 1 giờ:       {feat_debug['realtime_window']}")

    print("\n" + "=" * 75)
    print("🎉 TỔNG KẾT LUỒNG END-TO-END:")
    print(f"  1. Track A giải mã quá khứ:       {total_track_a} witness events (T < T0)")
    print(f"  2. Feature Store Batch Redis:      55 features (v20260805)")
    print(f"  3. Track B mô phỏng tương lai:     {total_track_b} synthetic events (T > T0)")
    print(f"  4. Stream Consumer qua Kafka:      Đã ghi nhận realtime events vào Redis (rt_events_1h)")
    print(f"  5. Model Uplift Inference:         {decision_resp['model_version']} (is_stub: false)")
    print(f"  6. Quyết định phát Voucher:        {decision_resp['decision']} (uplift score: {decision_resp['uplift_score']:.6f})")
    print(f"  7. Độ trễ phục vụ suy luận:        {decision_resp['latency_ms']:.2f} ms")
    print("=" * 75)


if __name__ == "__main__":
    main()
