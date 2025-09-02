import json
import time
import pika

messages = [
    {"symbol": "EURUSD-X", "bid": 1.16683, "ask": 1.16698, "time": "16:43:49"},
    {"symbol": "USDRY-X", "bid": 41.13920, "ask": 41.16120, "time": "16:43:35"},
    {"symbol": "XAUUSD-X", "bid": 3505.820, "ask": 3506.250, "time": "16:43:50"},
    {"symbol": "XAUUSDM25-X", "bid": 3570.160, "ask": 3570.350, "time": "16:43:50"},
]

connection = pika.BlockingConnection(pika.ConnectionParameters())
channel = connection.channel()
queue_name = "quotes"
channel.queue_declare(queue=queue_name, durable=True)

for msg in messages:
    channel.basic_publish(exchange="", routing_key=queue_name, body=json.dumps(msg).encode())
    time.sleep(1)

connection.close()
