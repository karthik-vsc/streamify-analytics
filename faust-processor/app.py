"""
Streamify Analytics — Phase 3b: Faust Stream Processor
───────────────────────────────────────────────────────
Compare this to processor.py (Phase 3a) and notice:
  • No manual Consumer/Producer setup
  • No poll() calls
  • No offset commit calls
  • No signal handling
  • KTable built in one line
  • Windowed aggregation built in one line
  Faust handles all of that internally.
"""

import config
import json
import time
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from datetime import timedelta
from collections import defaultdict
from pathlib import Path

import faust
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import(
    MessageField,
    SerializationContext
)

from metrics import(
   events_processed,events_enriched,buffer_spikes_detected,
   processing_errors,dlq_messages_sent,processing_latency,
   rocksdb_state_bytes,
)

app=faust.App(
    config.APP_ID,
    broker=config.BROKER_URL,
    topic_partitions=3,
    value_serializer="raw"  # we handle Avro deserialisation ourselves
)

# ── Topic declarations ─────────────────────────────────────────────────────
# Faust needs to know about topics before it can read/write them
viewing_events_topic=app.topic(config.VIEWING_EVENTS_TOPIC,value_type=bytes)
content_metadata_topic=app.topic(config.CONTENT_METADATA_TOPIC,value_type=bytes)
enriched_events_topic=app.topic(config.ENRICHED_EVENTS_TOPIC,value_type=bytes)
alerts_topic=app.topic(config.ALERTS_TOPIC,value_type=bytes)
dlq_topic=app.topic(config.VIEWING_EVENTS_TOPIC+"-dlq", value_type=bytes)

# ── Avro deserialiser setup ────────────────────────────────────────────────
def build_deserializers():
    sr_client=SchemaRegistryClient({"url":config.SCHEMA_REGISTRY_URL})

    viewing_schema=(Path(__file__).parent/"schemas"/"viewing_event.avsc").read_text()
    meta_schema=(Path(__file__).parent/"schemas"/"content_metadata.avsc").read_text()

    return(
        AvroDeserializer(sr_client,Schema(viewing_schema,"AVRO")),
        AvroDeserializer(sr_client,Schema(meta_schema,"AVRO"))
    )
viewing_deser,meta_deser=build_deserializers()


# ── KTable: content metadata ───────────────────────────────────────────────
# This single line replaces the entire load_metadata_table() function
# from Phase 3a (30+ lines of manual consumer code).
#
# Faust automatically:
#   • Consumes the content-metadata topic in the background
#   • Keeps the table in sync when new metadata arrives
#   • Persists the table to RocksDB so it survives restarts
metadata_table=app.Table(
    "content-metadata-table",
    default=dict
)

# ── Windowed table: active viewers ─────────────────────────────────────────
# This single line replaces the entire TumblingWindow class from Phase 3a.
#
# .tumbling(60) → 60-second non-overlapping time buckets
# .relative_to_stream() → window time based on event timestamp
# expires → how long to keep old window data in RocksDB
###Not implemented
viewer_counts=app.Table(
    "viewer-counts",
    default=int
).tumbling(
    config.TUMBLING_WINDOW_SEC,
    expires=timedelta(hours=1)
)

# ── Buffer spike tracker (still manual — Faust has no built-in for this) ───
buffer_times: dict[str,list]=defaultdict(list)

# ── Helper: enrich event ───────────────────────────────────────────────────
def enrich(event: dict)->dict:
    """Stream-table join — look up content metadata from Faust KTable."""
    content_id=event["content_id"]
    # Faust KTable lookup — same concept as Phase 3a dict lookup
    # but now Faust keeps this table automatically in sync
    meta=metadata_table.get(content_id,{})
    return{
      **event,
      "genre":meta.get("genre","UNKNOWN"),
      "duration_sec":meta.get("duration_sec",0),
      "maturity_rating":meta.get("maturity_rating","UNKNOWN"),
      "enriched_at_ms":int(time.time()*1000),
      "processor":"faust"   # so we can tell which processor enriched it
    }

# ── Helper: buffer spike detection ────────────────────────────────────────
def check_buffer_spike(content_id:str, title:str)->dict|None:
  """Same logic as Phase 3a BufferSpikeDetector, but as a plain function."""
  now=time.time()
  buffer_times[content_id].append(now)
  cutoff=now-config.BUFFER_ALERT_WINDOW
  buffer_times[content_id]=[
     t for t in buffer_times[content_id] if t>=cutoff
  ]

  if len(buffer_times[content_id])>=config.BUFFER_ALERT_COUNT:
    return {
      "alert_type":"BUFFER_SPIKE",
      "content_id":content_id,
      "title":title,
      "timestamp_ms": int(now * 1000),
      "message":f"Buffer spike detected on '{title}'",
      "processor":"faust"
    }
  return None

# ── Agent: metadata loader ─────────────────────────────────────────────────
# An "agent" in Faust is a consumer that runs continuously in the background.
# This one keeps the metadata_table in sync with the content-metadata topic.
# Compare to the 30-line load_metadata_table() function in Phase 3a.
"""
Faust internally does all of this and hands you the result as stream:
consumer = KafkaConsumer(content_metadata_topic)
stream   = AsyncIterator(consumer)
async for msg_bytes in stream:
You're just saying "give me the next message from the topic, one at a time." Faust handles:
polling Kafka
error handling
offset commits
rebalancing
"""
#faust calls these agent decorators automatically based on the command provided in the dockerfile
@app.agent(content_metadata_topic)
async def load_metadata(stream):
  async for msg_bytes in stream:
     meta=meta_deser(
        msg_bytes,
        SerializationContext(config.CONTENT_METADATA_TOPIC,MessageField.VALUE)
     )
     if meta:
        metadata_table[meta["content_id"]]=meta
        print(f"[META] Loaded: {meta['content_id']} → {meta['title']}")

# ── Agent: main stream processor ───────────────────────────────────────────
# This is the heart of the processor. Compare to the entire main() loop
# in Phase 3a — all the consumer setup, polling, error handling is gone.
# Faust gives us a clean async stream to iterate over.
@app.agent(viewing_events_topic)
async def process_viewing_events(stream):
  async for msg_bytes in stream:
    events_processed.labels(topic=config.VIEWING_EVENTS_TOPIC).inc()
    start=time.monotonic()
    event=None
    # 1 — Deserialise Avro bytes → Python dict
    try:
      event=viewing_deser(msg_bytes, SerializationContext(config.VIEWING_EVENTS_TOPIC,MessageField.VALUE))

    except Exception as e:
      processing_errors.labels(stage='deserialize',error_type=type(e).__name__).inc()
      dlq_messages_sent.labels(reason='deserialize_failed').inc()
      await dlq_topic.send(value=msg_bytes)
      processing_latency.observe(time.monotonic()-start)
      continue

    if not event:
      processing_latency.observe(time.monotonic() - start)
      continue
    # 2 — Enrich: stream-table join
    try:
      enriched_event=enrich(event)

      # 3 — Produce to enriched-events
      #Faust internally created and manages a producer for that topic. It handles batching, retries, poll, and flush automatically as part of the app lifecycle.
      schema = {
        "schema": {
            "type": "struct",
            "fields": [
              {"field": "event_id",        "type": "string", "optional": True},
              {"field": "user_id",         "type": "string", "optional": True},
              {"field": "content_id",      "type": "string", "optional": True},
              {"field": "content_title",   "type": "string", "optional": True},
              {"field": "event_type",      "type": "string", "optional": True},
              {"field": "timestamp_ms",    "type": "int64",  "optional": True},
              {"field": "genre",           "type": "string", "optional": True},
              {"field": "duration_sec",    "type": "int32",  "optional": True},
              {"field": "maturity_rating", "type": "string", "optional": True},
              {"field": "enriched_at_ms",  "type": "int64",  "optional": True},
              {"field": "processor",       "type": "string", "optional": True}
            ],
            "optional": False
        },
        "payload": enriched_event
      }
      await enriched_events_topic.send(
        key=event["user_id"].encode(),
        value=json.dumps(schema).encode("utf-8"),
      )
      events_enriched.inc()
      # 4 — Update windowed viewer count
      # Faust handles the time window automatically
      viewer_counts[event["content_id"]]+=1

      # 5 — Buffer spike detection
      if event["event_type"] == "BUFFER":
        alert = check_buffer_spike(event["content_id"], event["content_title"])
        if alert:
          print(f"[ALERT] 🚨 {alert['message']}")
          buffer_spikes_detected.inc()
          alert_schema = {
            "schema": {
              "type": "struct",
              "fields": [
                  {"field": "alert_type",   "type": "string", "optional": True},
                  {"field": "content_id",   "type": "string", "optional": True},
                  {"field": "title",        "type": "string", "optional": True},
                  {"field": "timestamp_ms", "type": "int64",  "optional": True},
                  {"field": "message",      "type": "string", "optional": True},
                  {"field": "processor",    "type": "string", "optional": True}
              ],
              "optional": False
            },
            "payload": alert
          }
          await alerts_topic.send(
              key=event["content_id"].encode(),
              value=json.dumps(alert_schema).encode("utf-8"),
          )
    
    except (KeyError, TypeError) as e:
      # exactly the field-name-mismatch class of bug you were chasing before
      processing_errors.labels(stage='enrichment',error_type=type(e).__name__).inc()
      dlq_messages_sent.labels(reason='missing_or_bad_field').inc()
      await dlq_topic.send(
        value=json.dumps(event,default=str).encode(),
        key=str(event.get("user_id","unknown")).encode()
      )
    except Exception as e:
      processing_errors.labels(stage='enrichment',error_type=type(e).__name__).inc()
      dlq_messages_sent.labels(reason='unexpected_error').inc()
      await dlq_topic.send(value=json.dumps(event, default=str).encode())
    finally:
      processing_latency.observe(time.monotonic()-start)

@app.page('/metrics')
async def metrics(self, request):
   return web.Response(body=generate_latest(),content_type='text/plain',charset='utf-8')

@app.timer(interval=30.0)
async def report_rocksdb_size():
  data_dir=Path(app.conf.datadir) #directory where Faust stores its local state( "/var/faust")
  if not data_dir.exists():
    return
  for table_dir in data_dir.glob('**/rocksdb'): #Search recursively for every folder named rocksdb.
    table_name=table_dir.parent.name
    total=sum(f.stat().st_size for f in table_dir.rglob('*') if f.is_file())
    rocksdb_state_bytes.labels(table=table_name).set(total)

# ── Timer: emit windowed viewer counts every 60 seconds ───────────────────
# Temporarily disabled — Faust's windowed-table iteration API is finicky
# across versions and this is a debug/console feature, not part of the
# core enrichment/alerting pipeline.
'''
@app.timer(interval=config.TUMBLING_WINDOW_SEC)
async def emit_viewer_counts():
  """Periodically log active viewer counts from the windowed table."""
  print("[WIN] ── Active viewers snapshot ──")
  """
  viewer_counts["c001"] = {
    window(18:00 → 18:01): 12,   # previous window
    window(18:01 → 18:02): 7,    # current window  ← .current() returns this
  }
  """
  for content_id, count in viewer_counts.items().now():
    if count > 0:
      print(f"[WIN]  {content_id} → {count} views this window")
'''