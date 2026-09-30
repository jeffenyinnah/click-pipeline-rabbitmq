import json
import queue
import socket
import tkinter as tk
import uuid
from datetime import datetime, timezone

import pika
from pynput import mouse

from topology import EXCHANGE, declare

DEVICE_ID = socket.gethostname()

# screen size, sent with every event so we can normalize coordinates later
root = tk.Tk()
root.withdraw()
SCREEN_W, SCREEN_H = root.winfo_screenwidth(), root.winfo_screenheight()
root.destroy()

pending = queue.Queue()   # hand-off between the listener thread and the publisher


def on_click(x, y, button, pressed):
    if not pressed:
        return
    pending.put({
        "event_id": str(uuid.uuid4()),
        "schema_version": 1,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "device_id": DEVICE_ID,
        "x": x,
        "y": y,
        "button": button.name,
        "screen_width": SCREEN_W,
        "screen_height": SCREEN_H,
    })


listener = mouse.Listener(on_click=on_click)
listener.start()

# --- RabbitMQ setup ---
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
declare(channel)             # exchanges, queues and the dead-letter setup live in topology.py
channel.confirm_delivery()

print(f"Publishing clicks from {DEVICE_ID} to '{EXCHANGE}'. Ctrl+C to stop.")
try:
    while True:
        try:
            event = pending.get(timeout=1)
        except queue.Empty:
            connection.process_data_events()   # keeps the connection alive while idle
            continue
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="mouse.click",
            body=json.dumps(event),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=event["event_id"],
            ),
        )
        print(f"{event['event_time']}  {event['button']} ({event['x']}, {event['y']})")
except KeyboardInterrupt:
    pass
finally:
    listener.stop()
    connection.close()