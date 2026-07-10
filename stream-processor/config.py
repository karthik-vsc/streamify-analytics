import os

KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")

#topics
VIEWING_EVENTS_TOPIC="viewing-events"
CONTENT_METADATA_TOPIC="content-metadata"
ENRICHED_EVENTS_TOPIC="enriched-events"
ALERTS_TOPIC="alerts"

SCHEMA_REGISTRY_URL=os.getenv("SCHEMA_REGISTRY_URL","http://localhost:8081")

# ── Consumer config ────────────────────────────────────────────────────────
# group.id → this processor belongs to consumer group "stream-processor-group"
#            Kafka will assign partitions to this group automatically
#
# auto.offset.reset → "earliest" means on first run, read all messages
#                     from the beginning, not just new ones
#
# enable.auto.commit → False means WE control when offsets are committed
#                      This is critical for exactly-once processing —
#                      we only commit after we've successfully processed
#                      AND produced the enriched event

CONSUMER_CONFIG={
    "bootstrap.servers":KAFKA_BOOTSTRAP_SERVERS,
    "group.id":"stream-processor-group",
    "auto.offset.reset":"earliest",
    "enable.auto.commit":False
}

PRODUCER_CONFIG={
    "bootstrap.servers":KAFKA_BOOTSTRAP_SERVERS,
    "acks":1,
    "linger.ms":5,
    "batch.size":16384,
    "compression.type":"snappy"
}

# ── Window settings ────────────────────────────────────────────────────────
TUMBLING_WINDOW_SEC=60 # aggregate active viewers every 60 seconds
BUFFER_ALERT_WINDOW=30 # seconds to watch for buffer spike
BUFFER_ALERT_COUNT=5 # how many BUFFER events trigger an alert