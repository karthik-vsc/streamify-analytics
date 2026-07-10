# Streamify Analytics 🎬

A production-grade real-time streaming analytics pipeline built on Apache Kafka, simulating the data infrastructure behind platforms like Netflix and Spotify.

## Architecture
FastAPI Producer → Apache Kafka → Stream Processors → Storage → Dashboards
├── Faust (Python)   ├── PostgreSQL
└── Kafka Streams    └── ClickHouse
↓
Kafka Connect
↓
Prometheus + Grafana

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 3.7 (KRaft mode) |
| Schema Management | Confluent Schema Registry + Avro |
| Stream Processing | Faust (Python), Kafka Streams (Java) |
| Event Producer | FastAPI (Python) |
| Sink Connectors | Kafka Connect (JDBC, ClickHouse) |
| Transactional DB | PostgreSQL 16 |
| Analytical DB | ClickHouse 24.3 |
| Observability | Prometheus + Grafana |
| Infrastructure | Docker Compose |

## What It Does

- Simulates **50 concurrent users** emitting real-time viewing events (play, pause, seek, buffer, complete, rate)
- Produces events to Kafka partitioned by `user_id` for guaranteed per-user ordering
- Enriches events via **stream-table join** with content metadata
- Detects **buffer spike anomalies** (CDN issues) using sliding window detection
- Computes **active viewer counts** per title using 60-second tumbling windows
- Sinks enriched events to PostgreSQL (transactional) and ClickHouse (analytical)
- Monitors pipeline health via Prometheus + Grafana dashboards

## Kafka Concepts Covered

- KRaft mode (no Zookeeper)
- Topic partitioning and replication
- Consumer groups and offset management
- Avro serialization with Schema Registry
- Stream-table joins
- Tumbling and sliding windows
- Exactly-once semantics
- Kafka Connect sink connectors
- Consumer lag monitoring

## Project Structure
streamify-analytics/
├── docker-compose.yml          # Full stack orchestration
├── producer/                   # FastAPI event simulator
│   ├── main.py
│   ├── config.py
│   └── schemas/
│       └── viewing_event.avsc  # Avro schema
├── stream-processor/           # Phase 3a: Plain Python processor
│   ├── processor.py
│   ├── metadata_producer.py
│   └── schemas/
├── faust-processor/            # Phase 3b: Faust stream processor
│   └── app.py
└── kafka-streams/              # Phase 4: Kafka Streams (Java)
├── pom.xml
└── src/

## Running Locally

**Prerequisites:** Docker Desktop, WSL2 (Windows)

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/streamify-analytics.git
cd streamify-analytics

# Start all services
docker compose up -d --build

# Wait ~60 seconds for everything to start, then seed content metadata
docker compose logs kafka-init   # verify topics created

# Start producing events
curl -X POST http://localhost:8000/simulate/start

# Check stats
curl http://localhost:8000/stats

# View live events in Kafka
docker exec -it streamify-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic viewing-events \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true
```

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/simulate/start` | POST | Start emitting 10 events/sec |
| `/simulate/stop` | POST | Stop simulation |
| `/stats` | GET | Live producer statistics |
| `/produce/single` | POST | Produce one manual event |
| `/catalogue` | GET | View content catalogue |
| `/docs` | GET | Swagger UI |

## Observability

- **Grafana:** http://localhost:3000 (admin/admin)
- **Prometheus:** http://localhost:9090
- **Schema Registry:** http://localhost:8081
- **Kafka Connect:** http://localhost:8083

## Key Learning Outcomes

- Built the same pipeline 3 ways (plain Python → Faust → Kafka Streams) to understand what each layer abstracts
- Debugged real Kafka Connect issues using Dead Letter Queue pattern
- Understood OLTP vs OLAP tradeoffs (PostgreSQL vs ClickHouse)
- Learned why ClickHouse uses `MergeTree` engine and `ORDER BY` as primary index
- Implemented graceful shutdown with SIGINT/SIGTERM handling
- Monitored consumer lag and broker health in production-style dashboards

## Phases

- [x] Phase 1: Kafka fundamentals + CLI exercises
- [x] Phase 2: FastAPI producer + Schema Registry + Avro
- [x] Phase 3a: Plain Python stream processor
- [x] Phase 3b: Faust stream processor
- [x] Phase 5: Kafka Connect → PostgreSQL + ClickHouse
- [x] Phase 6: Prometheus + Grafana observability
