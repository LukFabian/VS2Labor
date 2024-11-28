import sys
import zmq
from collections import Counter

me = f"Reducer-{sys.argv[1]}"

context = zmq.Context()
pull_socket = context.socket(zmq.PULL)
port = 5556 if sys.argv[1] == "1" else 5557
pull_socket.bind(f"tcp://*:{port}")

word_counts = Counter()

while True:
    word = pull_socket.recv_string()
    word_counts[word] += 1
    print(f"{me}: Counted '{word}' -> {word_counts[word]}")
