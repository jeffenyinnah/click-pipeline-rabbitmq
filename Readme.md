# Click Pipeline

A small data engineering project: system-wide mouse clicks are captured on a desktop,
streamed through RabbitMQ, archived as Parquet in an S3-compatible data lake (MinIO),
and queried with DuckDB. dbt models, orchestration and dashboards are next.

## Architecture

```
pynput listener -> SQLite outbox -> RabbitMQ -> consumer -> MinIO (Parquet) -> DuckDB
   (producer.py)                  (topic exchange)  (validate, batch)   (date= partitions)
                                        |
                                        +-> dead-letter queue (clicks.dead) for bad messages
```

## Stack

Python, pynput, RabbitMQ, MinIO, Parquet (pyarrow), Pydantic, DuckDB, Docker Compose.

## Design decisions

- **Outbox pattern:** clicks are written to a local SQLite file first and published second,
  so nothing is lost while the broker is down. A row is deleted only after the broker confirms it.
- **At-least-once delivery, dedupe downstream:** every event carries a UUID `event_id`;
  duplicates from retries are removed with `row_number()` in the query layer.
- **Dead-letter queue:** messages that fail validation are parked in `clicks.dead`
  instead of blocking the consumer.
- **Manual acks after durable writes:** the consumer acks a batch only after the Parquet
  objects are stored, so a crash redelivers instead of losing data.
- **Schema versioning:** `schema_version` is on every event. Version 2 fixed a
  display-scaling bug where screen size and click coordinates used different pixel units.
- **Hive-style partitions** (`raw/date=YYYY-MM-DD/`) so query engines can skip data.

## Run it

```
copy .env.example .env          # then edit the values if you like
docker compose up -d            # RabbitMQ (UI :15672) and MinIO (console :9001)
pip install -r requirements.txt
python consumer_archiver.py     # terminal 1
python producer.py              # terminal 2, then click around
python explore.py               # queries the lake
```

## Roadmap

- [x] Producer with local outbox and reconnect
- [x] Consumer with validation, DLQ, batching, reconnect
- [x] MinIO data lake + DuckDB queries
- [ ] `ingested_at` column to measure event lateness
- [ ] dbt models: staging, sessions, hourly aggregates, heatmap bins
- [ ] Orchestration (Dagster or Airflow) and data quality tests
- [ ] Grafana dashboards, load generator, failure drills