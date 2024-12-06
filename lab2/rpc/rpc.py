import constRPC
import time
import threading
from context import lab_channel


class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self

    def __repr__(self):
        return f"DBList({self.value})"


class Client:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.server = None
        self.response_callback = None

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')

    def stop(self, timeout=0):
        start_time = time.time()
        while time.time() - start_time < timeout:
            time.sleep(0.5)
        self.chan.leave('client')

    def set_response_callback(self, callback):
        """Set a callback function to handle responses from the server."""
        self.response_callback = callback

    def append(self, data, db_list, timeout=30):
        """Send an append request to the server asynchronously with a timeout."""
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        print("Client: Sending append request to server.")
        self.chan.send_to(self.server, msglst)  # send msg to server
        # Start a thread to wait for the server's response asynchronously
        threading.Thread(target=self._wait_for_response, args=(timeout,)).start()
        print("Client: Append request sent, continuing with other tasks...")

    def ack(self, timeout=30):
        msg = constRPC.OK  # message payload
        print("Client: Sending append request to server.")
        self.chan.send_to(self.server, msg)  # send msg to server
        # Start a thread to wait for the server's response asynchronously
        threading.Thread(target=self._wait_for_response, args=(timeout,)).start()
        print("Client: Append request sent, continuing with other tasks...")
    
    def _wait_for_response(self, timeout):
        """Wait for response from server within the specified timeout and call the callback."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            msgrcv = self.chan.receive_from(self.server, timeout=1)  # check periodically
            if msgrcv:  # Response received
                print("Client: Response received from server.")
                if self.response_callback:
                    self.response_callback(msgrcv[1])  # pass response to the callback
                return
            print("Client: Waiting for server response...")
        print("Client: Timeout reached. No response received from server.")


class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        return db_list.append(data)

    def run(self):
        self.chan.bind(self.server)
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters
                if constRPC.APPEND == msgrpc[0]:  # check what is being requested
                    print("Server: Append request received. Processing...")
                    result = self.append(msgrpc[1], msgrpc[2])  # do local call
                    print("Server: Simulating long processing time (10 seconds)...")
                    time.sleep(10)  # simulate long processing time
                    print("Server: Sending result back to client.")
                    self.chan.send_to({client}, result)  # return response
                elif constRPC.OK == msgrpc[0]:
                    self.chan.send_to({client}, constRPC.OK)
                else:
                    pass  # unsupported request, simply ignore
