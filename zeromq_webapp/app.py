import json
import socket
import threading
import time
from typing import Dict

from flask import Flask, jsonify, render_template

app = Flask(__name__)

# In-memory storage for quotes received from TCP clients.
quotes: Dict[str, Dict[str, str]] = {}
quotes_lock = threading.Lock()


def _handle_connection(conn: socket.socket) -> None:
    """Read newline-delimited JSON messages from a socket."""
    buffer = ""
    with conn:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data.decode()
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
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


def consume_from_tcp() -> None:
    """Background thread accepting JSON messages on TCP port 6565."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 6565))
    server.listen()
    while True:
        conn, _ = server.accept()
        threading.Thread(target=_handle_connection, args=(conn,), daemon=True).start()


# Start consumer thread as soon as module is imported.
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
