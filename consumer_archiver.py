import json
import time
from datetime import datetime
from pathlib import Path
from typing import Literal
from pydantic import AwareDatetime, BaseModel, ValidationError
from topology import declare

import pika
import pyarrow as pa
import pyarrow.parquet as pq

QUEUE = "clicks.raw"
BATCH_SIZE = 50
FLUSH_SECONDS = 10
OUT_DIR = Path("data/raw")

# explicit schema: fields missing from old messages become nulls instead of breaking the batch
SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("schema_version", pa.int32()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("device_id", pa.string()),
    ("x", pa.int32()),
    ("y", pa.int32()),
    ("button", pa.string()),
    ("screen_width", pa.int32()),
    ("screen_height", pa.int32()),
])

class ClickEvent(BaseModel):
    event_id: str
    schema_version: int
    event_time: AwareDatetime
    device_id: str
    x: int
    y: int
    button: Literal["left", "right", "middle"]
    screen_width: int
    screen_height: int

batch = []
last_tag = None
batch_started = None


def flush(channel):
    global batch, last_tag, batch_started

    # group by the date of the click, one folder per day
    by_date = {}
    for event in batch:
        by_date.setdefault(event["event_time"].date().isoformat(), []).append(event)

    for date, events in by_date.items():
        folder = OUT_DIR / f"date={date}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"clicks_{int(time.time() * 1000)}.parquet"
        pq.write_table(pa.Table.from_pylist(events, schema=SCHEMA), path)
        print(f"wrote {len(events)} events -> {path}")

    # ack only AFTER the files are safely on disk
    channel.basic_ack(delivery_tag=last_tag, multiple=True)
    batch, last_tag, batch_started = [], None, None


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
declare(channel)
channel.basic_qos(prefetch_count=BATCH_SIZE)   # broker sends at most one batch of unacked messages

print(f"Consuming '{QUEUE}' -> {OUT_DIR}/ . Ctrl+C to stop.")
try:
    for method, props, body in channel.consume(QUEUE, inactivity_timeout=1):
        if method is not None:
            try:
                event = ClickEvent.model_validate_json(body).model_dump()
            except ValidationError as err:
                print("bad message -> dead letter queue:", err.errors()[0]["msg"])
                channel.basic_reject(method.delivery_tag, requeue=False)
                continue
            if not batch:
                batch_started = time.time()
            batch.append(event)
            last_tag = method.delivery_tag

        if batch and (len(batch) >= BATCH_SIZE or time.time() - batch_started >= FLUSH_SECONDS):
            flush(channel)
except KeyboardInterrupt:
    pass
finally:
    connection.close()   # anything unacked goes back to the queue