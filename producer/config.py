import os
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC","viewing-events")
SCHEMA_REGISTRY_URL=os.getenv("SCHEMA_REGISTRY_URL","http://localhost:8081")

# ── Producer tuning ────────────────────────────────────────────────────────
# acks="all"  → leader + all ISR replicas must confirm before we consider
#               the write successful. Safest durability guarantee.
#
# enable.idempotence=True → Kafka assigns each producer a PID and a sequence
#               number per partition. If a retry arrives after the original
#               succeeded (network blip), the broker deduplicates it.
#               Together with acks=all this gives exactly-once producer
#               semantics within Kafka.
#
# linger.ms=5 → Wait up to 5ms before flushing a batch. Lets multiple
#               events accumulate in one network round-trip (throughput ↑).
#
# batch.size  → Max bytes per batch. 16 KB is a sensible starting point.

PRODUCER_CONFIG={
    "bootstrap.servers":KAFKA_BOOTSTRAP_SERVERS,
    "acks":"1",
    #"enable.idempotence":True,
    "linger.ms":5,
    "batch.size":16384,
    "compression.type":"snappy",
    "retries":3,
    "retry.backoff.ms":300,
}

# ── Simulation settings ────────────────────────────────────────────────────
NUM_USERS       = 50     # virtual concurrent users
NUM_CONTENT     = 20     # titles in our fake catalogue
EVENTS_PER_SEC  = 10     # target throughput