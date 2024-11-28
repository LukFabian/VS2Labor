import sys
import zmq
import hashlib

me = f"Mapper-{sys.argv[1]}"

context = zmq.Context()
pull_socket = context.socket(zmq.PULL)
pull_socket.connect("tcp://localhost:5555")  # Verbindung zum Splitter

push_socket1 = context.socket(zmq.PUSH)
push_socket2 = context.socket(zmq.PUSH)
push_socket1.connect("tcp://localhost:5556")  # Reducer 1
push_socket2.connect("tcp://localhost:5557")  # Reducer 2


def get_reducer(word):
    return push_socket1 if int(hashlib.md5(word.encode(), usedforsecurity=False).hexdigest(), 16) % 2 == 0 else push_socket2


while True:
    line = pull_socket.recv_string()
    print(f"{me}: Received line '{line}'")
    words = line.split()
    for word in words:
        reducer_socket = get_reducer(word)
        reducer_socket.send_string(word)
        print(f"{me}: Sent word '{word}' to Reducer")
