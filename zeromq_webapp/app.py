import json
import socketserver
import threading
import time
from typing import Any, Dict

from flask import Flask, jsonify, render_template
import zmq

app = Flask(__name__)

# In-memory storage for quotes received from ZeroMQ and plain TCP.
quotes: Dict[str, Dict[str, str]] = {}
quotes_lock = threading.Lock()


def store_quote(message: Dict[str, Any]) -> None:
    """Normalize and store a quote message.

    Extracts the required fields from *message* and updates the global
    ``quotes`` dictionary so that the frontend can display the data. The
    incoming JSON is expected to contain at least a ``local_symbol`` or
    ``symbol`` field and may optionally include ``bidprice``/``bid``,
    ``askprice``/``ask`` and ``time``.
    """
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
        store_quote(message)


class QuoteTCPHandler(socketserver.StreamRequestHandler):
    """Handle incoming plain TCP connections with JSON lines.

    Each line received is expected to contain a JSON document representing a
    quote. Multiple clients can connect concurrently thanks to
    :class:`~socketserver.ThreadingTCPServer`.
    """

    def handle(self) -> None:  # pragma: no cover - simple integration logic
        for line in self.rfile:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                continue
            store_quote(message)


def consume_from_tcp() -> None:
    """Background thread consuming JSON messages from TCP port 6565."""
    server = socketserver.ThreadingTCPServer(("127.0.0.1", 6565), QuoteTCPHandler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


# Start consumer threads as soon as the module is imported.
threading.Thread(target=consume_from_zeromq, daemon=True).start()
threading.Thread(target=consume_from_tcp, daemon=True).start()


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
    app.run(host="172.31.28.98", port=80, debug=True)
