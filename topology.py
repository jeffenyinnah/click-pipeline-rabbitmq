EXCHANGE = "mouse.events"
DLX = "mouse.events.dlx"
RAW_QUEUE = "clicks.raw"
DEAD_QUEUE = "clicks.dead"


def declare(channel):
    # dead-letter side: a fanout exchange feeding a parking queue
    channel.exchange_declare(exchange=DLX, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DLX)

    # main side: same as before, plus "send rejects to the DLX"
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(
        queue=RAW_QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": DLX},
    )
    channel.queue_bind(queue=RAW_QUEUE, exchange=EXCHANGE, routing_key="mouse.#")