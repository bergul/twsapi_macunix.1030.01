import json
import time
import zmq

messages = [
    {
        "local_symbol": "GCZ5",
        "bidprice": 3573.7,
        "askprice": 3573.8,
        "time": "2025-09-02T15:12:33.1428118Z",
    }
]

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5555")
time.sleep(0.5)

for msg in messages:
    socket.send_string(json.dumps(msg))
    time.sleep(1)

socket.close()
context.term()
