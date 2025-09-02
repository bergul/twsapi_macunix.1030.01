import json
import time
import pika

messages = [
    {
        "local_symbol": "GCZ5",
        "bidprice": 3573.7,
        "askprice": 3573.8,
        "time": "2025-09-02T15:12:33.1428118Z",
    }
]

connection = pika.BlockingConnection(pika.ConnectionParameters())
channel = connection.channel()
queue_name = "quotes"
channel.queue_declare(queue=queue_name, durable=True)

for msg in messages:
    channel.basic_publish(exchange="", routing_key=queue_name, body=json.dumps(msg).encode())
    time.sleep(1)

connection.close()
