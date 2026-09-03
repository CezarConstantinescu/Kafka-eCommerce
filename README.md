# Unified Retail Analytics pipeline with Kafka

This repository demonstrates a simple, production-minded real-time analytics
pipeline built on Apache Kafka (KRaft mode, ZooKeeperless). 
Three data owners each produce a stream; several
consumers persist raw data to disk and a centralized analytics 
consumer aggregates KPIs across all streams.

Key goals:
- Illustrate producers with delivery reporting and meaningful partitioning
- Deploy a 3-broker Kafka cluster in Docker (RF=3)
- Show consumer group behavior, manual commits, replayability, and durability
- Produce an offline analysis pipeline and a simple HTML dashboard

Quickly: the code reads CSV sources, produces to three topics, consumes to
JSONL files, runs analytics over the JSONL or live Kafka stream, and
generates an HTML dashboard.

Contents
- `docker-compose.yml` — 3-broker KRaft cluster and helper services
- `producer_*.py` — producers for each data source
- `consumer_*.py` — raw-sink consumers persisting JSONL
- `consumer_analytics.py` — analytics consumer that subscribes to all topics
- `analyze_jsonl.py` — offline aggregator that reads the JSONL sinks
- `generate_html_report.py` — renders `analytics_report.json` into `analytics_dashboard.html`
- `*.csv` — sample data sources (`ecommerce.csv`, `sales.csv`, `product_reviews.csv`)

Architecture (high level)

Producers
- `producer_ecommerce.py` → topic `ecommerce_events` (key: `Order ID`)
- `producer_sales.py`     → topic `sales_events`     (key: `CustomerID`)
- `producer_reviews.py`   → topic `product_reviews`  (key: `ProductID`)

Each topic is created with 3 partitions and replication factor 3 to match the
3-broker Docker cluster. Meaningful keys ensure related events (same order,
customer, or product) land in the same partition for correct per-key ordering.

Consumers
- Three raw-sink consumers write consumed records as one JSON object per line
  into `*_events.jsonl` files. They use `enable.auto.commit=False` and commit
  offsets only after the message is flushed to disk (at-least-once semantics).
- `consumer_analytics.py` subscribes to all three topics under a separate
  group (`analytics-consumer-group`). It writes periodic snapshots and a final
  `analytics_report.json` plus a human-readable `analytics_summary.txt`.

Offline analysis
- `analyze_jsonl.py` reads the three JSONL sinks and reproduces the analytics
  report without connecting to Kafka. Useful for reproducible experiments.

Durability & delivery
- Producers use `acks=all` to ensure the leader waits for ISR acknowledgements.
- Topics created with RF=3 and `min.insync.replicas=2` (see `docker-compose.yml`)
  to balance durability and availability.
- Consumers disable auto-commit and only commit after successful persistence to avoid
  data loss after a crash.

How to run
Prerequisites
- Docker Desktop (or Docker Engine) running
- Python 3.9+ and `pip install -r requirements.txt` (or `pip install confluent-kafka`)

Start the cluster
```
docker compose up -d
```
This brings up three KRaft brokers and any required helper containers.

Produce data
Run each producer in a separate terminal to stream its CSV file into Kafka:
```
python producer_ecommerce.py
python producer_sales.py
python producer_reviews.py
```

Consume to raw sinks
Run the three raw consumers in separate terminals:
```
python consumer_ecommerce.py
python consumer_sales.py
python consumer_reviews.py
```
Each consumer writes a `.jsonl` file and commits offsets after writes succeed.

Run analytics consumer (live)
```
python consumer_analytics.py
```
It subscribes to all topics, prints incoming records, and writes periodic and
final reports to `analytics_report.json` and `analytics_summary.txt`.

Offline analytics and dashboard
```
python analyze_jsonl.py
python generate_html_report.py
```
The first reads the JSONL files and writes `analytics_report.json`. The second
renders `analytics_dashboard.html` and opens it in your browser.

Consumer group demo
Start the same consumer script twice (same `group.id`) to observe Kafka's
rebalance and partition assignment. Stop one instance to see ownership shift.

Useful CLI commands
- List topics: `docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list`
- Describe a topic: `docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic ecommerce_events`
- Read messages: use `kafka-console-consumer` inside a broker container with `--from-beginning`
- List/describe consumer groups: `kafka-consumer-groups` (see the code comments for exact examples)

Design choices and trade-offs
- KRaft (no ZooKeeper) simplifies local docker orchestration for a demo
- RF=3 and `acks=all` maximize durability for the example, but increase
  write latency and resource use — suitable for analytics workloads
- Manual offset commit favors at-least-once delivery. For exactly-once, use
  Kafka transactions and idempotent producers (out of scope for this demo)

Troubleshooting
- If topics do not appear, check broker logs: `docker compose logs kafka-1`
- If producers fail with authentication or connection errors, ensure
  `BOOTSTRAP_SERVERS` in the producer script matches the compose service names
- To replay a consumer group from the start:
  `docker exec -it kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 --group analytics-consumer-group --topic ecommerce_events --reset-offsets --to-earliest --execute`

Files of interest
- `docker-compose.yml` — cluster configuration and replication settings
- `producer_*.py` — shows `delivery_report()` and per-message key usage
- `consumer_*.py` and `consumer_analytics.py` — show manual commits and persistence
- `analyze_jsonl.py` — offline aggregation logic
- `generate_html_report.py` — simple static dashboard generator