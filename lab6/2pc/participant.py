import random
import logging

# coordinator messages
from const2PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT
# participant decisions
from const2PC import LOCAL_SUCCESS, LOCAL_ABORT, PREPARE_COMMIT, READY_COMMIT, NEW_COORDINATOR, STATE_CHANGE
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT
# misc constants
from const2PC import TIMEOUT

import stablelog


class Participant:
    """
    Implements a two phase commit participant.
    - state written to stable log (but recovery is not considered)
    - in case of coordinator crash, participants mutually synchronize states
    - system blocks if all participants vote commit and coordinator crashes
    - allows for partially synchronous behavior with fail-noisy crashes
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log(
            "participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.2pc.Participant")
        self.coordinator = {}
        self.all_participants = set()
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not
        return LOCAL_ABORT if random.random() > 2 / 3 else LOCAL_SUCCESS

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Participant {} entered state {}."
                         .format(self.participant, state))
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self._enter_state('INIT')  # Start in local INIT state.

    def run(self):
        # Wait for start of joint commit
        msg = self.channel.receive_from(self.coordinator, TIMEOUT * 2)
        smallest_process_id = min([int(participant) for participant in self.all_participants])
        smallest_process_id = min(smallest_process_id, int(self.participant))

        if not msg:  # Crashed coordinator - give up entirely
            # decide to locally abort (before doing anything)
            decision = LOCAL_ABORT
            self._enter_state('ABORT')

        else:  # Coordinator requested to vote, joint commit starts
            assert msg[1] == VOTE_REQUEST

            # Firstly, come to a local decision
            decision = self._do_work()  # proceed with local activities

            # If local decision is negative,
            # then vote for abort and quit directly
            if decision == LOCAL_ABORT:
                self.logger.info(f"participant {self.participant} in state {self.state}: local decision is negative")
                self.channel.send_to(self.coordinator, VOTE_ABORT)
                self._enter_state('ABORT')

            # If local decision is positive,
            # we are ready to proceed the joint commit
            else:
                assert decision == LOCAL_SUCCESS
                self._enter_state('READY')
                self.channel.send_to(self.coordinator, VOTE_COMMIT)

            pre_commit_message = self.channel.receive_from(self.coordinator, TIMEOUT)
            if not pre_commit_message:
                self.logger.info(
                    f"participant {self.participant} in state {self.state}: replacing crashed coordinator {self.coordinator} with {smallest_process_id}")
                self.all_participants.remove(str(smallest_process_id))
                self.coordinator = {str(smallest_process_id)}
                if str(smallest_process_id) == self.participant:
                    self.logger.info(
                        f"participant {self.participant} in state {self.state}: is new coordinator")
                    if self.state == 'READY':
                        self.channel.send_to(self.all_participants, VOTE_REQUEST)
                        # Collect votes from all participants
                        yet_to_receive = list(self.all_participants)
                        while len(yet_to_receive) > 0:
                            msg = self.channel.receive_from(self.all_participants, TIMEOUT)

                            if (not msg) or (msg[1] == VOTE_ABORT):
                                reason = "timeout on state WAIT" if not msg else "local_abort from " + msg[0]
                                self._enter_state('ABORT')
                                # Inform all participants about global abort
                                self.channel.send_to(self.all_participants, GLOBAL_ABORT)
                                return "New Coordinator {} terminated in state ABORT. Reason: {}." \
                                    .format(self.coordinator, reason)
                            else:
                                assert msg[1] == VOTE_COMMIT
                                yet_to_receive.remove(msg[0])
                        self._enter_state('COMMIT')
                        decision = GLOBAL_COMMIT
                        self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
                    elif self.state == 'PRECOMMIT':
                        self._enter_state('COMMIT')
                        decision = GLOBAL_COMMIT
                        self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
                    elif self.state == 'ABORT':
                        self.channel.send_to(self.all_participants, GLOBAL_ABORT)
            elif pre_commit_message[1] == GLOBAL_ABORT:
                self._enter_state('ABORT')
            elif pre_commit_message[1] == PREPARE_COMMIT:
                self._enter_state('PRECOMMIT')
                self.channel.send_to(self.coordinator, READY_COMMIT)
                decision = self.channel.receive_from(self.coordinator, TIMEOUT)[1]
                if decision == GLOBAL_COMMIT:
                    self._enter_state('COMMIT')
        if self.participant != str(smallest_process_id):
            msg = self.channel.receive_from(self.coordinator, TIMEOUT * 2)
            if msg:
                self.logger.info(
                    f"participant {self.participant} in state {self.state}: received {msg[1]} from newly appointed coordinator")
                if msg[1] == GLOBAL_ABORT:
                    decision = GLOBAL_ABORT
                    self._enter_state('ABORT')
                elif msg[1] == GLOBAL_COMMIT:
                    decision = GLOBAL_COMMIT
                    self._enter_state('COMMIT')
                elif msg[1] == VOTE_REQUEST:
                    if self.state == 'ABORT':
                        self.channel.send_to(self.coordinator, VOTE_ABORT)
                    else:
                        self.channel.send_to(self.coordinator, VOTE_COMMIT)
                        msg = self.channel.receive_from(self.coordinator, TIMEOUT)
                        if msg:
                            if msg[1] == GLOBAL_COMMIT:
                                self._enter_state('COMMIT')
                            elif msg[1] == GLOBAL_ABORT:
                                self._enter_state('ABORT')
                            decision = msg[1]

        return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
