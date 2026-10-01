import time
from typing import Literal

import pika
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pika.exceptions import AMQPError
from pydantic import AwareDatetime, BaseModel, ValidationError

from lake import BUCKET, RAW_PREFIX, get_fs
from topology import RAW_QUEUE, declare

BATCH_SIZE = 50
FLUSH_SECONDS = 10

fs = get_fs()

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
delay = 1   # retry backoff, doubles up to 30s


def ensure_bucket():
    if fs.get_file_info(BUCKET).type == pafs.FileType.NotFound:
        fs.create_dir(BUCKET)
        print(f"created bucket '{BUCKET}'")


def flush(channel):
    global batch, last_tag, batch_started

    # group by the date of the click, one prefix per day
    by_date = {}
    for event in batch:
        by_date.setdefault(event["event_time"].date().isoformat(), []).append(event)

    for date, events in by_date.items():
        path = f"{RAW_PREFIX}/date={date}/clicks_{int(time.time() * 1000)}.parquet"
        pq.write_table(pa.Table.from_pylist(events, schema=SCHEMA), path, filesystem=fs)
        print(f"wrote {len(events)} events -> s3://{path}")

    # ack only AFTER the objects are safely stored
    channel.basic_ack(delivery_tag=last_tag, multiple=True)
    batch, last_tag, batch_started = [], None, None


def consume_forever():
    """Connect and consume until RabbitMQ or MinIO fails (raises AMQPError / OSError)."""
    global batch, last_tag, batch_started, delay

    ensure_bucket()
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    try:
        channel = connection.channel()
        declare(channel)
        channel.basic_qos(prefetch_count=BATCH_SIZE)
        delay = 1
        print(f"connected. Consuming '{RAW_QUEUE}' -> s3://{RAW_PREFIX}/ . Ctrl+C to stop.")

        for method, props, body in channel.consume(RAW_QUEUE, inactivity_timeout=1):
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
    finally:
        if connection.is_open:
            connection.close()   # anything unacked goes back to the queue


try:
    while True:
        try:
            consume_forever()
        except (AMQPError, OSError) as err:
            # either the broker or the object store is down. Unacked messages go back
            # to the queue and will be redelivered, so drop the half-built batch.
            batch, last_tag, batch_started = [], None, None
            print(f"dependency unavailable ({type(err).__name__}). Retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)
except KeyboardInterrupt:
    print("stopped.")