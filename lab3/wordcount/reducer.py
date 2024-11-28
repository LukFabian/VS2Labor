import sys
import zmq
from collections import Counter
import time
import pickle

me = f"Reducer-{sys.argv[1]}"

context = zmq.Context()
pull_socket = context.socket(zmq.PULL)
port = 5556 if sys.argv[1] == "1" else 5557
pull_socket.bind(f"tcp://*:{port}")

collector_socket = context.socket(zmq.PUSH)
collector_socket.connect("tcp://localhost:5558")  # Collector-Adresse

word_counts = Counter()

while True:
    try:
        word = pull_socket.recv_string(flags=zmq.NOBLOCK)
        word_counts[word] += 1
    except zmq.Again:
        # Keine neuen Wörter; Zwischenergebnisse an Collector senden
        if word_counts:
            collector_socket.send(pickle.dumps((me, dict(word_counts))))
            print(f"{me}: Sent intermediate results to Collector")
            word_counts.clear()
        time.sleep(1)