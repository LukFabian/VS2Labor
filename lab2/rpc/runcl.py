import rpc
import logging
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


# Callback-Funktion zur Verarbeitung des Ergebnisses des RPC-Aufrufs
def response_handler(result):
    print("Result: {}".format(result.value))


# Client-Setup und Start
cl = rpc.Client()
cl.run()

# Setzen der Callback-Funktion
cl.set_response_callback(response_handler)

# Erstellen der initialen DBList und Starten eines asynchronen Append-Requests mit Timeout
base_list = rpc.DBList(['foo'])
cl.append('bar', base_list, timeout=30)
cl.stop(timeout=30)
