#!/usr/bin/env bash
# Crea los topics de la POC: 4 topics de negocio + sus DLQ.
# 3 particiones, replication factor 1 (broker unico), retencion de 7 dias.
# La clave de los mensajes es customerId (la fija cada productor).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
RETENTION_MS=$((7 * 24 * 60 * 60 * 1000)) # 7 dias

TOPICS=(
  activation.requested
  billing.events
  provisioning.events
  activation.events
)

create_topic() {
  local topic="$1"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config "retention.ms=$RETENTION_MS" \
    --config "cleanup.policy=delete"
  echo "[create-topics] topic ${topic}: ${PARTITIONS} particiones, retencion 7 dias"
}

echo "[create-topics] esperando a Kafka en ${BOOTSTRAP}..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  sleep 1
done

for topic in "${TOPICS[@]}"; do
  create_topic "$topic"
  create_topic "${topic}.dlq"
done

echo "[create-topics] listo. Topics actuales:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
