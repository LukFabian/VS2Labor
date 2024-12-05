import sys
import zmq
from collections import Counter
import pickle

me = f"Reducer-{sys.argv[1]}"

context = zmq.Context()
pull_socket = context.socket(zmq.PULL)
port = 5556 if sys.argv[1] == "1" else 5557
pull_socket.bind(f"tcp://*:{port}")

collector_socket = context.socket(zmq.PUSH)
collector_socket.connect("tcp://localhost:5558")  # Collector-Adresse

word_counts = Counter()
poller = zmq.Poller()
poller.register(pull_socket, zmq.POLLIN)

sent_words = False
timeout_ms = 5000  # 5-second timeout for no new words

print(f"{me}: Waiting for words...")

while not sent_words:
    events = dict(poller.poll(timeout_ms))  # Wait for input with a timeout
    if pull_socket in events:
        try:
            word = pull_socket.recv_string()
            word_counts[word] += 1
        except zmq.ZMQError as e:
            print(f"{me}: Error receiving word: {e}")
    else:
        # No new words during the timeout period
        if word_counts:
            collector_socket.send(pickle.dumps((me, dict(word_counts))))
            print(f"{me}: Sent intermediate results to Collector")
            word_counts.clear()
            sent_words = True

pull_socket.close()
collector_socket.close()
