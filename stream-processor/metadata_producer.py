"""
Streamify Analytics — Content Metadata Producer
────────────────────────────────────────────────
Runs once to seed the content-metadata topic with our fake catalogue.
This topic is log-compacted — Kafka keeps only the latest record per
key (content_id), so re-running this script safely updates records.
"""

import json
import config
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import(
    MessageField,
    SerializationContext,
    StringSerializer
)

CATALOGUE = [
    {"content_id": "c001", "title": "Interstellar",     "genre": "SCI_FI",       "release_year": 2014, "duration_sec": 10140, "maturity_rating": "PG-13"},
    {"content_id": "c002", "title": "The Crown",        "genre": "DRAMA",         "release_year": 2016, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c003", "title": "Squid Game",       "genre": "THRILLER",      "release_year": 2021, "duration_sec": 3240,  "maturity_rating": "TV-MA"},
    {"content_id": "c004", "title": "Ozark",            "genre": "THRILLER",      "release_year": 2017, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c005", "title": "Dark",             "genre": "SCI_FI",        "release_year": 2017, "duration_sec": 3360,  "maturity_rating": "TV-MA"},
    {"content_id": "c006", "title": "Stranger Things",  "genre": "SCI_FI",        "release_year": 2016, "duration_sec": 3120,  "maturity_rating": "TV-14"},
    {"content_id": "c007", "title": "The Witcher",      "genre": "ACTION",        "release_year": 2019, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c008", "title": "Money Heist",      "genre": "THRILLER",      "release_year": 2017, "duration_sec": 3360,  "maturity_rating": "TV-MA"},
    {"content_id": "c009", "title": "Breaking Bad",     "genre": "DRAMA",         "release_year": 2008, "duration_sec": 2940,  "maturity_rating": "TV-MA"},
    {"content_id": "c010", "title": "Mindhunter",       "genre": "THRILLER",      "release_year": 2017, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c011", "title": "Black Mirror",     "genre": "SCI_FI",        "release_year": 2011, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c012", "title": "Peaky Blinders",   "genre": "DRAMA",         "release_year": 2013, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c013", "title": "Narcos",           "genre": "DRAMA",         "release_year": 2015, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c014", "title": "The Last of Us",   "genre": "ACTION",        "release_year": 2023, "duration_sec": 4200,  "maturity_rating": "TV-MA"},
    {"content_id": "c015", "title": "Succession",       "genre": "DRAMA",         "release_year": 2018, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c016", "title": "Ted Lasso",        "genre": "COMEDY",        "release_year": 2020, "duration_sec": 2400,  "maturity_rating": "TV-MA"},
    {"content_id": "c017", "title": "Severance",        "genre": "THRILLER",      "release_year": 2022, "duration_sec": 3600,  "maturity_rating": "TV-MA"},
    {"content_id": "c018", "title": "Andor",            "genre": "ACTION",        "release_year": 2022, "duration_sec": 3600,  "maturity_rating": "TV-14"},
    {"content_id": "c019", "title": "The Bear",         "genre": "DRAMA",         "release_year": 2022, "duration_sec": 1800,  "maturity_rating": "TV-MA"},
    {"content_id": "c020", "title": "Shogun",           "genre": "DRAMA",         "release_year": 2024, "duration_sec": 4200,  "maturity_rating": "TV-MA"},
]

def main():
    schema_path=Path(__file__).parent/"schemas"/"content_metadata.avsc" #Path(__file__).parent returns the directory containing current file
    schema_str=schema_path.read_text()

    sr_client=SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_ser=AvroSerializer(sr_client,Schema(schema_str,"AVRO"))
    key_ser=StringSerializer("utf-8")
    producer=Producer(config.PRODUCER_CONFIG)

    print(f"[META] Seeding {len(CATALOGUE)} titles into '{config.CONTENT_METADATA_TOPIC}'...")

    for item in CATALOGUE:
        producer.produce(
            topic=config.CONTENT_METADATA_TOPIC,
            key=key_ser(item["content_id"],SerializationContext(config.CONTENT_METADATA_TOPIC,MessageField.KEY)),
            value=avro_ser(item,SerializationContext(config.CONTENT_METADATA_TOPIC,MessageField.VALUE))
        )
        print(f"  → {item['content_id']} | {item['title']} | {item['genre']}")

    producer.flush()
    print("[META] Done. Content catalogue is in Kafka.")

if __name__=="__main__":
    main()