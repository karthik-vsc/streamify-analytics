import os

KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
VIEWING_EVENTS_TOPIC   = "viewing-events"
CONTENT_METADATA_TOPIC = "content-metadata"
ENRICHED_EVENTS_TOPIC  = "enriched-events"
ALERTS_TOPIC           = "alerts"

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

APP_ID="streamify-faust-processor"
BROKER_URL=f"kafka://{KAFKA_BOOTSTRAP_SERVERS}"

TUMBLING_WINDOW_SEC=60
BUFFER_ALERT_WINDOW=30
BUFFER_ALERT_COUNT=5