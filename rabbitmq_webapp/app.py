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
    {"symbol": "EURUSD-X", "bid": 1.11683, "ask": 1.16698, "time": "16:43:49"}
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters())
    channel = connection.channel()

    queue_name = "quotes"
    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        symbol = message.get("symbol") or message.get("local_symbol")
        if not symbol:
            return
        bid = message.get("bid") or message.get("bidprice", "")
        ask = message.get("ask") or message.get("askprice", "")
        msg_time = message.get("time", time.strftime("%H:%M:%S"))
        with quotes_lock:
            quotes[symbol] = {
                "symbol": symbol,
                "bid": str(bid),
                "ask": str(ask),
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
