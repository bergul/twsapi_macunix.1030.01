import json
import threading
import time
from typing import Dict

from flask import Flask, jsonify, render_template
import pika

app = Flask(__name__)

# In-memory storage for quotes received from RabbitMQ.
quotes: Dict[str, Dict[str, str]] = {}
quotes_lock = threading.Lock()


def consume_from_rabbitmq() -> None:
    """Background thread consuming quotes from RabbitMQ.

    The consumer expects messages in JSON format like:
    {"local_symbol": "GCZ5", "bidprice": 3573.7, "askprice": 3573.8,
     "time": "2025-09-02T15:12:33.1428118Z"}
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters())
    channel = connection.channel()

    queue_name = "quotes"
    channel.queue_declare(queue=queue_name, durable=True)
    # Explicitly bind the queue to the default exchange so messages published
    # with routing_key=queue_name are received.
    channel.queue_bind(queue=queue_name, exchange="", routing_key=queue_name)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        symbol = message.get("local_symbol") or message.get("symbol")
        if not symbol:
            return
        bid = message.get("bidprice") or message.get("bid") or ""
        ask = message.get("askprice") or message.get("ask") or ""
        msg_time = message.get("time", time.strftime("%H:%M:%S"))
        with quotes_lock:
            quotes[symbol] = {
                "local_symbol": symbol,
                "bidprice": str(bid),
                "askprice": str(ask),
                "time": msg_time,
            }

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


# Start consumer thread as soon as module is imported.
threading.Thread(target=consume_from_rabbitmq, daemon=True).start()


@app.route("/")
def index():
    """Render the main table view."""
    return render_template("index.html")


@app.route("/data")
def data_endpoint():
    """Provide the latest quotes as JSON for the frontend."""
    with quotes_lock:
        return jsonify(list(quotes.values()))


if __name__ == "__main__":
    app.run(debug=True)
