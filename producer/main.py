"""
Streamify Analytics — Phase 2: FastAPI Viewing-Event Producer
─────────────────────────────────────────────────────────────
What this file does:
  1. Registers an Avro schema with Schema Registry on startup
  2. Exposes REST endpoints to start / stop the event simulation
  3. Simulates 50 virtual users watching content and emits events
     to the 'viewing-events' Kafka topic, partitioned by user_id
 
Key Kafka concepts exercised here:
  • Avro serialisation via Schema Registry (schema evolution safe)
  • Partitioning by key (user_id → same partition → ordered per user)
  • Idempotent producer + acks=all (exactly-once producer guarantees)
  • Delivery report callback (know if a message actually landed)
"""

import asyncio
import json
import random
import time
#a built-in Python library used to generate Universally Unique Identifiers (UUIDs), which are 128-bit numbers represented as 36-character alphanumeric strings
import uuid
#contextlib module helps you manage resources safely and cleanly.A context manager ensures that setup code runs before a block of code and cleanup code runs afterward, even if an exception occurs.
from contextlib import asynccontextmanager
from datetime import datetime,timezone
#module used for working with file and directory paths in an easy, readable, and cross-platform way.
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
#AvroSerializer converts your Python object into Avro binary format according to your Avro schema
from confluent_kafka.schema_registry.avro import AvroSerializer
#imports helper classes used to serialize Kafka messages before sending them to the broker.
from confluent_kafka.serialization import(
    MessageField,
    SerializationContext,
    StringSerializer,
)
from faker import Faker
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import config

# ── Globals ────────────────────────────────────────────────────────────────
fake=Faker()
producer=None   # confluent_kafka Producer
avro_ser=None   # Avro serialiser tied to Schema Registry
key_ser=StringSerializer("utf_8")

# creating global variables that keep track of your simulation's current state
sim_task: asyncio.Task | None=None
stats={
    "produced":0,
    "errors":0,
    "started_at":None,
    "running":False,
}

# ── Fake catalogue ─────────────────────────────────────────────────────────
CONTENT_CATALOGUE =[
    {"id":f"c{i:03}","title":title} #The format specifier :03 means: Use at least 3 digits.Pad with leading zeros if needed.Ex: c001,c002,c003
    for i,title in enumerate([
        "Interstellar", "The Crown", "Squid Game", "Ozark", "Dark",
        "Stranger Things", "The Witcher", "Money Heist", "Breaking Bad",
        "Mindhunter", "Black Mirror", "Peaky Blinders", "Narcos",
        "The Last of Us", "Succession", "Ted Lasso", "Severance",
        "Andor", "The Bear", "Shogun",
    ], start=1)
]

USERS=[f"user_{i:04}" for i in range(1,config.NUM_USERS+1)] #user_0001,user_0002
DEVICE_TYPES  = ["MOBILE", "DESKTOP", "TV", "TABLET"]
QUALITIES     = ["480p", "720p", "1080p", "4K"]
EVENT_WEIGHTS = {          # realistic event frequency weights.if we choose event using random.choice(events) then each event has 14% prob which is unrealistic so we are giving weightage for each event in realistic way.
    "PLAY":     30,
    "PAUSE":    20,
    "SEEK":     15,
    "BUFFER":   10,
    "COMPLETE": 10,
    "RATE":      5,
    "ERROR":     5,
    # remaining 5 % is silence (user idle)
}

# ── Schema Registry bootstrap ──────────────────────────────────────────────
def init_schema_registry()->AvroSerializer:
    """
    Register our Avro schema with Schema Registry.
 
    Schema Registry stores a versioned history of schemas under a 'subject'
    (by default: <topic>-value). When a consumer reads a message, it uses
    the schema ID embedded in the message bytes to fetch the exact schema
    version used to write it — even if the schema has evolved since.
    """
    schema_path=Path(__file__).parent / "schemas" / "viewing_event.avsc" #e builds the absolute path to your Avro schema file (viewing_event.avsc) in a way that works regardless of where you run the program from.
    schema_str=schema_path.read_text()

    sr_client=SchemaRegistryClient({"url":config.SCHEMA_REGISTRY_URL})
    # AvroSerializer handles:
    #   • Registering the schema if it's new
    #   • Embedding the schema ID as the first 5 bytes of every message
    #   • Serialising Python dicts to Avro binary
    from confluent_kafka.schema_registry import Schema
    return AvroSerializer(
        sr_client,
        Schema(schema_str,"AVRO")
    )

# ── Event factory ──────────────────────────────────────────────────────────
def make_event(user_id:str,session_id:str,content:dict)->dict:
    """Build one realistic viewing event for a given user + content."""
    event_type=random.choices(list(EVENT_WEIGHTS.keys()),weights=list(EVENT_WEIGHTS.values()))[0]

    event={
        "event_id":str(uuid.uuid4()),
        "user_id":user_id,
        "session_id":session_id,
        "content_id":content["id"],
        "content_title":content["title"],
        "event_type":event_type,
        "playback_position_sec":round(random.uniform(0, 7200), 2),
        "timestamp_ms":int(time.time()*1000),
        "device_type":random.choice(DEVICE_TYPES),
        "quality":random.choice(QUALITIES) if event_type != "ERROR" else None,
        "rating":round(random.uniform(1, 5), 1) if event_type == "RATE" else None,
        "buffer_duration_ms":  random.randint(200, 8000) if event_type == "BUFFER" else None
    }

    return event

# ── Delivery report callback ───────────────────────────────────────────────
def on_delivery(err,msg):
    """
    Called by librdkafka after a message is acknowledged (or fails).
 
    This is the correct way to know a message actually landed in Kafka.
    Never assume produce() means the message is safe — it's async.
    """
    if err:
        stats["errors"]+=1
        print(f"[Error] delivery failed: {err}")
    else:
        stats["produced"]+=1
        print(f"[OK] {msg.topic()}[{msg.partition()}] offset={msg.offset()}")

#async def makes your program more responsive by allowing other tasks to run whenever your code is waiting (for sleep, network I/O, file I/O, etc.).
async def simulate():
    """
    Continuously emit events at ~EVENTS_PER_SEC until cancelled.
 
    Each 'user' has an active session. Every tick we pick a random user,
    build an event, and produce it. The user_id is the Kafka message key —
    Kafka hashes it to decide the partition, so all events for the same
    user always land in the same partition (guaranteed ordering per user).
    """
    sessions={uid:str(uuid.uuid4()) for uid in USERS}
    content_choices={uid:random.choice(CONTENT_CATALOGUE) for uid in USERS}
    interval=1.0/config.EVENTS_PER_SEC
    stats["running"]=True
    stats["started_at"]=datetime.now(timezone.utc).isoformat()
    print(f"[SIM] Starting simulation: {config.NUM_USERS} users, "
          f"{config.EVENTS_PER_SEC} events/sec")
    
    try:
        while True:
            user_id=random.choice(USERS)
            event=make_event(
                user_id,
                sessions[user_id],
                content_choices[user_id]
            )
            # Occasionally change content (simulates user switching shows)
            if event["event_type"]=="COMPLETE" or random.random()<0.02:
                content_choices[user_id]=random.choice(CONTENT_CATALOGUE)
                sessions[user_id]=str(uuid.uuid4())

            # ── The actual Kafka produce call ──────────────────────────
            # key   = user_id  → determines partition (consistent ordering)
            # value = Avro-serialised event dict
            #producer.produce() queues the message for sending.
            #producer.poll() lets the Kafka client do background work such as actually sending batches, handling retries, and triggering delivery callbacks.
            producer.produce(
                topic=config.KAFKA_TOPIC,
                key=key_ser(user_id,SerializationContext(config.KAFKA_TOPIC,MessageField.KEY)),
                value=avro_ser(event,SerializationContext(config.KAFKA_TOPIC,MessageField.VALUE)),
                on_delivery=on_delivery
            )
            # poll() lets librdkafka fire delivery callbacks + send batches
            producer.poll(0)
            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        # Flush ensures all buffered messages are sent before we stop
        print("[SIM] Stopping — flushing remaining messages...")
        producer.flush(timeout=10)
        stats["running"]=False
        print("[SIM] Stopped")

@asynccontextmanager
async def lifespan(app:FastAPI):
    """Initialise Kafka producer + Schema Registry on startup."""
    global producer,avro_ser
    print("[BOOT] Connecting to Schema Registry...")
    avro_ser=init_schema_registry()
    print("[BOOT] Schema registered ✓")

    print("[BOOT] Creating Kafka producer...")
    producer=Producer(config.PRODUCER_CONFIG)
    print("[BOOT] Producer ready ✓")

    yield #app runs here
    
    if sim_task and not sim_task.done():
        sim_task.cancel()
    if producer:
        producer.flush()

app=FastAPI(
    title="Streamify Analytics — Event Producer",
    description="Simulates real-time viewing events and produces them to Kafka",
    version="1.0.0",
    lifespan=lifespan
)

# ── Endpoints ──────────────────────────────────────────────────────────────
@app.post("/simulate/start",summary="Strart emitting viewing points")
async def start_simulation():
    global sim_task
    if sim_task and not sim_task.done():
        raise HTTPException(status_code=400,detail="Simulation already running")
    sim_task=asyncio.create_task(simulate())
    return{"status":"started","events_per_sec":config.EVENTS_PER_SEC}

@app.post("/simulate/stop",summary="Stop the simulation")
async def stop_simulation():
    global sim_task
    if not sim_task or sim_task.done():
        raise HTTPException(status_code=400,detail="Simulation is not running")
    
    sim_task.cancel()
    return {"status":"stopped","stats":stats}

@app.get("/stats",summary="Live producer statistics")
async def get_stats():
    return{
        **stats, #copies all key-value pairs from one dictionary into another
        "topic":config.KAFKA_TOPIC,
        "users":config.NUM_USERS,
        "content":config.NUM_CONTENT
    }

@app.post("/produce/single", summary="Produce one event manually (good for testing)")
async def produce_single(user_id: str | None = None, event_type: str | None = None, content_id: str | None = None):
    """Lets you fire a single hand-crafted event without running the full simulation."""
    uid = user_id or random.choice(USERS)
    if content_id:
        content = next((c for c in CONTENT_CATALOGUE if c["id"] == content_id), None)
        if content is None:
            raise HTTPException(400, f"content_id '{content_id}' not found in catalogue")
    else:
        content = random.choice(CONTENT_CATALOGUE)
    event = make_event(uid, str(uuid.uuid4()), content)
    if event_type:
        valid = list(EVENT_WEIGHTS.keys())
        if event_type.upper() not in valid:
            raise HTTPException(400, f"event_type must be one of {valid}")
        event["event_type"] = event_type.upper()
    producer.produce(
        topic=config.KAFKA_TOPIC,
        key=key_ser(uid, SerializationContext(config.KAFKA_TOPIC, MessageField.KEY)),
        value=avro_ser(event, SerializationContext(config.KAFKA_TOPIC, MessageField.VALUE)),
        on_delivery=on_delivery,
    )
    producer.flush()
    return {"status": "produced", "event": event}

@app.get("/health")
async def health():
    return {"status":"ok","producer_ready":producer is not None}

@app.get("/catalogue",summary="See the fake content catalogue")
async def get_catalogue():
    return CONTENT_CATALOGUE
