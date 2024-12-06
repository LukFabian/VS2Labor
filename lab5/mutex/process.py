import logging
import random
import time
import threading
from constMutex import ENTER, RELEASE, ALLOW, CHECK, ACK


class Process:
    """
    Implements access management to a critical section (CS) via fully
    distributed mutual exclusion (MUTEX).

    Processes broadcast messages (ENTER, ALLOW, RELEASE) timestamped with
    logical (lamport) clocks. All messages are stored in local queues sorted by
    logical clock time.

    A process broadcasts an ENTER request if it wants to enter the CS. A process
    that doesn't want to ENTER replies with an ALLOW broadcast. A process that
    wants to ENTER and receives another ENTER request replies with an ALLOW
    broadcast (which is then later in time than its own ENTER request).

    A process enters the CS if a) its ENTER message is first in the queue (it is
    the oldest pending message) AND b) all other processes have sent messages
    that are younger (either ENTER or ALLOW). RELEASE requests purge
    corresponding ENTER requests from the top of the local queues.

    Message Format:

    <Message>: (Timestamp, Process_ID, <Request_Type>)

    <Request Type>: ENTER | ALLOW  | RELEASE

    """

    def __init__(self, chan):
        self.channel = chan  # Create ref to actual channel
        self.process_id = self.channel.join('proc')  # Find out who you are
        self.all_processes: list = []  # All procs in the proc group
        self.other_processes: list = []  # Needed to multicast to others
        self.failed_processes = set()
        self.queue = []  # The request queue list
        self.clock = 0  # The current logical clock
        self.logger = logging.getLogger("vs2lab.lab5.mutex.process.Process")

    def __mapid(self, id='-1'):
        # resolve channel member address to a human friendly identifier
        if id == '-1':
            id = self.process_id
        return 'Proc_' + chr(65 + self.all_processes.index(id))

    def __cleanup_queue(self):
        if len(self.queue) > 0:
            # self.queue.sort(key = lambda tup: tup[0])
            self.queue.sort()
            while len(self.queue) > 0 and self.queue[0][2] == ALLOW:
                del (self.queue[0])

    def __request_to_enter(self):
        self.clock += 1
        request_msg = (self.clock, self.process_id, ENTER)
        self.queue.append(request_msg)
        self.__cleanup_queue()
        self.channel.send_to(self.other_processes, request_msg)

    def __allow_to_enter(self, requester):
        self.clock += 1
        msg = (self.clock, self.process_id, ALLOW)
        self.channel.send_to([requester], msg)

    def __release(self):
        # need to be first in queue to issue a release
        assert self.queue[0][1] == self.process_id, 'State error: inconsistent local RELEASE'
        self.queue = [r for r in self.queue[1:] if r[2] == ENTER]
        self.clock += 1
        msg = (self.clock, self.process_id, RELEASE)
        # Multicast release notification
        self.channel.send_to(self.other_processes, msg)

    def __allowed_to_enter(self):
        # See who has sent a message (the set will hold at most one element per sender)
        processes_with_later_message = set([req[1] for req in self.queue[1:]])
        # Access granted if this process is first in queue and all others have answered (logically) later
        first_in_queue = self.queue[0][1] == self.process_id
        all_have_answered = len(self.other_processes) - len(self.failed_processes) == len(processes_with_later_message)
        return first_in_queue and all_have_answered

    def __receive(self):
        # Pick up any message
        _receive = self.channel.receive_from(self.other_processes, 10)
        if _receive:
            msg = _receive[1]
            if msg[2] == CHECK:
                self.logger.debug(f"{self.process_id} received CHECK from {self.__mapid(msg[1])}")
                self.channel.send_to(msg[1], (self.process_id, ACK))
            else:
                self.clock = max(self.clock, msg[0]) + 1

                self.logger.debug("{} received {} from {}.".format(
                    self.__mapid(),
                    "ENTER" if msg[2] == ENTER
                    else "ALLOW" if msg[2] == ALLOW
                    else "RELEASE", self.__mapid(msg[1])))

                if msg[2] == ENTER:
                    self.queue.append(msg)
                    self.__allow_to_enter(msg[1])
                elif msg[2] == ALLOW:
                    self.queue.append(msg)
                elif msg[2] == RELEASE:
                    assert self.queue[0][1] == msg[1] and self.queue[0][
                        2] == ENTER, 'State error: inconsistent remote RELEASE'
                    del (self.queue[0])
                self.__cleanup_queue()
        else:
            self.logger.warning("{} timed out on RECEIVE. Checking for failures.".format(self.__mapid()))
            self.__check_for_failures()

    def __check_for_failures(self):
        """Detect failed processes using CHECK and ACK messages."""
        working_processes = list()
        checkup_msg = (self.clock, self.process_id, CHECK)
        ack_responses = []

        # Create a thread to listen for ACK messages
        def listen_for_acks():
            while True:
                try:
                    ack_msg = self.channel.receive_from(self.other_processes, 10)
                    if ack_msg and ack_msg[1] == ACK:
                        ack_responses.append(ack_msg[0])  # Collect ACKs
                except Exception as e:
                    self.logger.debug(f"ACK listening thread encountered an error: {e}")
                    break
            # Start listening thread

        ack_listener = threading.Thread(target=listen_for_acks, daemon=True)
        ack_listener.start()

        # Send CHECK messages to all other processes
        self.channel.send_to(self.other_processes, checkup_msg)

        # Wait for ACK responses (allowing time for ACKs to arrive)
        ack_listener.join(timeout=10)

        # Evaluate which processes responded
        working_processes = [proc for proc in self.other_processes if proc in ack_responses]
        failed_processes = set(self.other_processes) - set(working_processes)

        if failed_processes:
            self.logger.warning(f"Detected failure of processes: {failed_processes}")

        self.other_processes = working_processes

    def init(self):
        self.channel.bind(self.process_id)
        self.all_processes = sorted(self.channel.subgroup('proc'), key=lambda x: int(x))
        self.other_processes = [proc for proc in self.all_processes if proc != self.process_id]
        self.logger.info("Member {} joined channel as {}.".format(self.process_id, self.__mapid()))

    def run(self):
        while True:
            if len(self.other_processes) > 1 and random.choice([True, False]):
                self.logger.debug("{} wants to ENTER CS at CLOCK {}.".format(self.__mapid(), self.clock))
                self.__request_to_enter()
                while not self.__allowed_to_enter():
                    self.__receive()

                # Stay in CS for some time ...
                sleep_time = random.randint(0, 2000)
                self.logger.debug("{} enters CS for {} milliseconds.".format(self.__mapid(), sleep_time))
                print(" CS <- {}".format(self.__mapid()))
                time.sleep(sleep_time / 1000)

                # ... then leave CS
                print(" CS -> {}".format(self.__mapid()))
                self.__release()
                continue

            # Occasionally serve requests to enter (
            if random.choice([True, False]):
                self.__receive()
