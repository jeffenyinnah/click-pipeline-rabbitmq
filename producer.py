import ctypes
import json
import socket
import sqlite3
import threading
import time
import tkinter as tk
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika
from pika.exceptions import AMQPError
from pynput import mouse

from topology import EXCHANGE, declare

DEVICE_ID = socket.gethostname()
DB_PATH = Path(__file__).parent / "outbox.db"
BATCH = 100

# Windows can scale the screen for tkinter (e.g. a 1920x1080 display reported as
# 1536x864) while pynput reports real pixels. Opt in to real pixels so they agree.
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(2)
except Exception:
    pass   # not on Windows

# screen size, sent with every event
root = tk.Tk()
root.withdraw()
SCREEN_W, SCREEN_H = root.winfo_screenwidth(), root.winfo_screenheight()
root.destroy()

# --- the outbox: one SQLite connection shared by both threads, guarded by a lock ---
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute(
    "CREATE TABLE IF NOT EXISTS outbox ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT, body TEXT NOT NULL)"
)
db.commit()
db_lock = threading.Lock()


def outbox_size():
    with db_lock:
        return db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]


# --- listener thread: only ever writes to the outbox, never touches RabbitMQ ---
def on_click(x, y, button, pressed):
    if not pressed:
        return
    event = {
        "event_id": str(uuid.uuid4()),
        "schema_version": 2,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "device_id": DEVICE_ID,
        "x": x,
        "y": y,
        "button": button.name,
        "screen_width": SCREEN_W,
        "screen_height": SCREEN_H,
    }
    with db_lock:
        db.execute(
            "INSERT INTO outbox (event_id, body) VALUES (?, ?)",
            (event["event_id"], json.dumps(event)),
        )
        db.commit()


listener = mouse.Listener(on_click=on_click)
listener.start()


# --- publisher loop (main thread): outbox -> RabbitMQ ---
def connect():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    declare(channel)
    channel.confirm_delivery()
    return connection, channel


connection = None
channel = None
delay = 1   # reconnect backoff, doubles up to 30s

print(f"Outbox at {DB_PATH} ({outbox_size()} events waiting). Ctrl+C to stop.")
try:
    while True:
        try:
            if connection is None or connection.is_closed:
                connection, channel = connect()
                print("connected to RabbitMQ")
                delay = 1

            with db_lock:
                rows = db.execute(
                    "SELECT id, event_id, body FROM outbox ORDER BY id LIMIT ?", (BATCH,)
                ).fetchall()

            if not rows:
                connection.process_data_events(time_limit=1)   # idle: heartbeats + wait
                continue

            for row_id, event_id, body in rows:
                channel.basic_publish(          # raises if the broker doesn't confirm
                    exchange=EXCHANGE,
                    routing_key="mouse.click",
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                        message_id=event_id,
                    ),
                )
                with db_lock:                    # delete only AFTER the confirm
                    db.execute("DELETE FROM outbox WHERE id = ?", (row_id,))
                    db.commit()
            print(f"published {len(rows)} events ({outbox_size()} still waiting)")

        except AMQPError as err:
            connection = None
            print(f"RabbitMQ unavailable ({type(err).__name__}). "
                  f"{outbox_size()} events safe in outbox. Retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)
except KeyboardInterrupt:
    pass
finally:
    listener.stop()
    if connection is not None and connection.is_open:
        connection.close()
    db.close()