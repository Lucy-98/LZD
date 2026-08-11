#!/bin/bash
# Tao topic voi cau hinh ro rang (khong dua vao auto-create) - idempotent.
set -e

BROKER="kafka:9092"

create_topic () {
  local name=$1 partitions=$2 retention_ms=$3
  if kafka-topics --bootstrap-server "$BROKER" --list | grep -qx "$name"; then
    echo "[kafka-init] topic '$name' da ton tai -> bo qua"
  else
    kafka-topics --bootstrap-server "$BROKER" --create \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor 1 \
      --config retention.ms="$retention_ms" \
      --config cleanup.policy=delete
    echo "[kafka-init] tao topic '$name' (partitions=$partitions)"
  fi
}

# Event chinh tu app: 3 partition de demo consumer lag / parallelism
create_topic "$KAFKA_TOPIC_EVENTS" 3 172800000     # giu 2 ngay
# Dead letter queue cho event hong schema
create_topic "$KAFKA_TOPIC_DLQ" 1 604800000        # giu 7 ngay

echo "[kafka-init] danh sach topic hien tai:"
kafka-topics --bootstrap-server "$BROKER" --list
