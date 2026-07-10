"""
Streamify Analytics — Phase 3: Stream Processor
────────────────────────────────────────────────
What this does:
  1. Loads content-metadata into an in-memory lookup table (KTable pattern)
  2. Consumes viewing-events one by one
  3. Enriches each event with genre + duration from the lookup table
  4. Tracks active viewers per title in a 60-second tumbling window
  5. Detects buffer spikes and fires alerts

Key Kafka concepts exercised here:
  • Manual offset commit (we control exactly-once behaviour)
  • Stream-table join (viewing-events + content-metadata)
  • Tumbling windows (stateful time-based aggregation)
  • Anomaly detection pattern
"""

import config
import json
import signal #a built-in library used to intercept and handle asynchronous operating system signals
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer,Producer,KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
    StringDeserializer
)

# ── Graceful shutdown ──────────────────────────────────────────────────────
running=True

def handle_shutdown(sig, frame):
    global running
    print("\n[PROC] Shutdown signal received, draining...")
    running=False

signal.signal(signal.SIGINT,handle_shutdown) #Signal Interrupt(Ctrl+C)
signal.signal(signal.SIGTERM,handle_shutdown) #Signal Terminate

# ── Serialisers / Deserialisers ────────────────────────────────────────────
def build_serializers():
    sr_client=SchemaRegistryClient({"url":config.SCHEMA_REGISTRY_URL})
    # Deserialiser for incoming viewing events
    viewing_schema=(Path(__file__).parent/"schemas"/"viewing_event.avsc").read_text()
    viewing_deser=AvroDeserializer(sr_client,Schema(viewing_schema,"AVRO"))

    # Deserialiser for content metadata lookup
    meta_schema=(Path(__file__).parent/"schemas"/"content_metadata.avsc").read_text()
    meta_deser=AvroDeserializer(sr_client,Schema(meta_schema,"AVRO"))

    # Serialiser for enriched events output (reuse viewing schema + extra fields as JSON)
    key_ser=StringSerializer("utf_8")
    key_deser=StringDeserializer("utf_8")

    return viewing_deser,meta_deser,key_ser,key_deser

# ── Step 1: Load content-metadata into memory (KTable pattern) ─────────────
def load_metadata_table(meta_deser,key_deser)->dict:
    """
    Read the entire content-metadata topic into a Python dict.
    This is the KTable pattern — a compacted topic loaded as a
    local in-memory lookup table for fast stream-table joins.

    In production Kafka Streams does this automatically with
    RocksDB as the state store. Here we do it manually so you
    see exactly what's happening.
    """
    print("[PROC] Loading content-metadata table...")

    consumer=Consumer({
        **config.CONSUMER_CONFIG,
        "group.id":"metadata-loader-group", # overwriting the group and using diff group cause if we use same group kafka treats the consumer as part of same group and assigns each partition to each consumer(here this consumer is performing diff use case) resulting in loss of data
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([config.CONTENT_METADATA_TOPIC])

    table={}
    empty_polls=0
    while empty_polls<5:
        msg=consumer.poll(timeout=2.0) #reads any new messages

        if msg is None:
            empty_polls+=1
            continue
        if msg.error():
            print(f"[PROC] Metadata error: {msg.error()}")
            continue

        key=key_deser(msg.key(),None)
        value=meta_deser(msg.value(),SerializationContext(config.CONTENT_METADATA_TOPIC,MessageField.VALUE))

        if value:
            table[key]=value
            empty_polls=0

    consumer.close()
    print(f"[PROC] Loaded {len(table)} titles into metadata table ✓")
    return table

def enrich_event(event:dict,metadata_table:dict)->dict:
    """
    Stream-table join: look up content_id in our in-memory table
    and add genre + duration to the viewing event.
    """
    meta=metadata_table.get(event["content_id"],{})
    return{
        **event,
        "genre":meta.get("genre","UNKNOWN"),
        "duration_sec":meta.get("duration_sec",0),
        "maturity_acting":meta.get("maturity_acting","UNKNOWN"),
        "enriched_at_ms":int(time.time()*1000)
    }

# ── Step 3: Tumbling window — active viewers per title ─────────────────────
class TumblingWindow:
    """
    Counts unique active viewers per content title in a fixed time window.

    A tumbling window is a non-overlapping, fixed-size time bucket.
    Every TUMBLING_WINDOW_SEC seconds the window 'turns over':
      → emit the counts for the just-closed window
      → start fresh counts for the new window

    Timeline example (60-sec window):
      [00:00 → 00:59]  Stranger Things: 12 viewers, Narcos: 8 viewers  → emit
      [01:00 → 01:59]  Stranger Things: 15 viewers, Narcos: 6 viewers  → emit
    """
     
    def __init__(self):
        self.window_start=time.time()
        # content_id → set of unique user_ids seen this window
        self.viewer_counts:dict[str,set]=defaultdict(set)
    
    def add(self,content_id,user_id):
        self.viewer_counts[content_id].add(user_id)
    
    def should_emit(self)->bool:
        return (time.time()-self.window_start)>=config.TUMBLING_WINDOW_SEC
    
    def emit_and_reset(self)->list[dict]:
        window_end=time.time()
        results=[
            {
                "window_start_ms":int(self.window_start*1000),
                "window_stop_ms":int(window_end*1000),
                "content_id":content_id,
                "active_viewers":len(viewers)
            }
            for content_id,viewers in self.viewer_counts.items()
        ]
        self.window_start=window_end
        self.viewer_counts=defaultdict(set)
        return results
    
# ── Step 4: Buffer spike detector ──────────────────────────────────────────
class BufferSpikeDetector:
    """
    Fires an alert if a title gets BUFFER_ALERT_COUNT+ BUFFER events
    within BUFFER_ALERT_WINDOW seconds.

    Sliding approach: stores timestamps of recent BUFFER events per
    content_id and checks if enough fall within the window.
    """
    def __init__(self):
        # content_id → list of timestamps of recent BUFFER events
        self.buffer_times: dict[str, list] = defaultdict(list)

    def record(self, content_id: str) -> bool:
        """Record a BUFFER event. Returns True if alert threshold crossed."""
        now = time.time()
        self.buffer_times[content_id].append(now)

        # Keep only events within the window
        cutoff = now - config.BUFFER_ALERT_WINDOW
        self.buffer_times[content_id] = [
            t for t in self.buffer_times[content_id] if t >= cutoff
        ]

        return len(self.buffer_times[content_id]) >= config.BUFFER_ALERT_COUNT
    
def main():
    viewing_deser,meta_deser,key_ser,key_deser=build_serializers()
    # Load the content catalogue into memory
    metadata_table=load_metadata_table(meta_deser,key_deser)

    # Set up consumer for viewing-events
    consumer=Consumer(config.CONSUMER_CONFIG)
    consumer.subscribe([config.VIEWING_EVENTS_TOPIC])

    # Set up producer for enriched-events + alerts
    producer=Producer(config.PRODUCER_CONFIG)

    window=TumblingWindow()
    detector=BufferSpikeDetector()
    # Counters
    processed=0
    enriched=0
    alerts=0

    print(f"[PROC] Listening on '{config.VIEWING_EVENTS_TOPIC}'...")

    while running:
        msg=consumer.poll(timeout=1.0)

        if msg is None:
            if window.should_emit():
                for agg in window.emit_and_reset():
                    print(f"[WIN]  {agg['content_id']} → {agg['active_viewers']} active viewers")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue   # normal — reached end of partition
            print(f"[ERROR] {msg.error()}")
            continue
        # ── Deserialise the incoming event ─────────────────────────────────
        event=viewing_deser(
            msg.value(),
            SerializationContext(config.VIEWING_EVENTS_TOPIC,MessageField.VALUE)
        )

        if not event:
            continue

        processed+=1

        # ── Enrich: stream-table join ──────────────────────────────────────
        enriched_event=enrich_event(event,metadata_table)

        # ── Produce to enriched-events ─────────────────────────────────────
        producer.produce(
            topic=config.ENRICHED_EVENTS_TOPIC,
            key=key_ser(event["user_id"],SerializationContext(config.ENRICHED_EVENTS_TOPIC,MessageField.KEY)),
            value=json.dumps(enriched_event).encode('utf-8')
        )
        enriched+=1

        # ── Tumbling window: track active viewers ──────────────────────────
        window.add(event["content_id"], event["user_id"])

        if window.should_emit():
            for agg in window.emit_and_reset():
                print(f"[WIN]  {agg['content_id']} → {agg['active_viewers']} active viewers")

        # ── Buffer spike detection ─────────────────────────────────────────
        if event["event_type"]=="BUFFER":
            if detector.record(event["content_id"]):
                alert={
                    "alert_type":"BUFFER_SPIKE",
                    "content_id":event["content_id"],
                    "title":event["content_title"],
                    "timestamp_ms":int(time.time()*1000),
                    "message":f"Buffer spike detected on '{event['content_title']}'"
                }
                
                producer.produce(
                    topic=config.ALERTS_TOPIC,
                    key=key_ser(event["content_id"],SerializationContext(config.ALERTS_TOPIC,MessageField.KEY)),
                    value=json.dumps(alert).encode("utf-8")
                )
                alerts+=1
                print(f"[ALERT] 🚨 {alert['message']}")
        # ── Manual offset commit ───────────────────────────────────────────
        # We only commit AFTER successfully processing + producing.
        # If we crash before this line, Kafka will re-deliver the message.
        # This gives us at-least-once processing guarantees.
        consumer.commit(asynchronous=False) #This tells Kafka: I have successfully finished processing this message. You can move my offset forward.
        producer.poll(0) #Triggers internal message sending,  producer's internal library (librdkafka) to process any pending callbacks — specifically delivery report callbacks — without waiting at all (0 means zero milliseconds timeout).
        if processed % 100 == 0:
            print(f"[PROC] processed={processed} enriched={enriched} alerts={alerts}")
    # ── Graceful shutdown ──────────────────────────────────────────────────
    print("[PROC] Flushing producer...")
    producer.flush() #waits until there are no outstanding messages left
    consumer.close()
    print(f"[PROC] Done. processed={processed} enriched={enriched} alerts={alerts}")

if __name__ == "__main__":
    main()
