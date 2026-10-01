import json
import uuid
from datetime import datetime, timezone

import pika

from topology import EXCHANGE, declare

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
declare(channel)
channel.confirm_delivery()

good_shape = {
    "event_id": str(uuid.uuid4()),
    "schema_version": 1,
    "event_time": datetime.now(timezone.utc).isoformat(),
    "device_id": "test",
    "x": 10, "y": 10,
    "button": "left",
    "screen_width": 1536, "screen_height": 864,
}

bad_messages = [
    b"this is not json at all",
    json.dumps({**good_shape, "x": "not a number"}).encode(),
    json.dumps({**good_shape, "button": "banana"}).encode(),
]

for body in bad_messages:
    channel.basic_publish(exchange=EXCHANGE, routing_key="mouse.click", body=body)

print(f"sent {len(bad_messages)} bad messages")
connection.close()