import json
import threading
import time
from typing import Dict

from flask import Flask, jsonify, render_template
import zmq

app = Flask(__name__)

# In-memory storage for quotes received from ZeroMQ.
quotes: Dict[str, Dict[str, str]] = {}
quotes_lock = threading.Lock()


def consume_from_zeromq() -> None:
    """Background thread consuming quotes from a ZeroMQ publisher.

    The consumer expects each message to be a JSON string such as:
    {"local_symbol": "GCZ5", "bidprice": 3573.7, "askprice": 3573.8,
     "time": "2025-09-02T15:12:33.1428118Z"}
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    socket.subscribe("")

    while True:
        message = json.loads(socket.recv_string())
        symbol = message.get("local_symbol") or message.get("symbol")
        if not symbol:
            continue
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


# Start consumer thread as soon as module is imported.
threading.Thread(target=consume_from_zeromq, daemon=True).start()


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
