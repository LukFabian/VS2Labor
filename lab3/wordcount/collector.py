import zmq
import pickle

context = zmq.Context()
pull_socket = context.socket(zmq.PULL)
pull_socket.bind("tcp://*:5558")  # Bind für Reducer-Verbindungen

final_word_counts = {}

print("Collector: Waiting for results from Reducers...")

while True:
    message = pull_socket.recv()
    reducer_name, partial_counts = pickle.loads(message)
    print(f"Collector: Received results from {reducer_name}")

    # Merge partial counts into final word count
    for word, count in partial_counts.items():
        if word in final_word_counts:
            final_word_counts[word] += count
        else:
            final_word_counts[word] = count

    # Output the current state of final_word_counts
    print("Collector: Current word counts:")
    for word, count in sorted(final_word_counts.items()):
        print(f"  {word}: {count}")
