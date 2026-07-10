"""Prometheus metrics for streamify-analytics Faust processor."""
from prometheus_client import Counter,Gauge,Histogram

#Throughtput
events_processed=Counter(
    'streamify_events_processed_total',
    'Total events processed by topic',
    ['topic']
)

events_enriched=Counter(
    'streamify_events_enriched_total',
    'Events successfully joined with content-metadata'
)

buffer_spikes_detected=Counter(
    'streamify_buffer_spikes_total',
    'Buffer spikes alerts fired'
)

# Errors / DLQ
processing_errors=Counter(
    'streamify_processing_errors_total',
    'Processing errors by stage and error type',
    ['stage','error_type']
)

dlq_messages_sent=Counter(
    'streamify_dlq_messages_total',
    'Messages routed to dead-letter queue',
    ['reason']
)

#latency
processing_latency=Histogram(
    'streamify_processing_latency_seconds',
    'Time to process a single event through enrichment agent',
    buckets=[.005, .01, .025, .05, .1, .25, .5, 1, 2.5]
)

#state
rocksdb_state_bytes=Gauge(
    'streamify_rocksdb_bytes',
    'On-disk size of RocksDB state stores',
    ['table']
)

active_partitions=Gauge(
    'streamify_active_partitions',
    'Number of partitions currently assigned to this worker'
)