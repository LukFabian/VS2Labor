import sys
import zmq

me = "Splitter"

context = zmq.Context()
push_socket = context.socket(zmq.PUSH)
push_socket.bind("tcp://*:5555")  # connect to mapper

if len(sys.argv) < 2:
    print("Usage: python Splitter.py <filename>")
    sys.exit()

filename = sys.argv[1]

with open(filename, 'r') as file:
    for line in file:
        push_socket.send_string(line.strip())
        print(f"{me}: Sent line to mappers")
