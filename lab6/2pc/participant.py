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
        self.all_participants = {}
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

    def appoint_new_coordinator(self):
        self.logger.info(
            f"participant {self.participant} in state {self.state}: coordinator crashed, trying to appoint new coordinator")
        # Ask all processes for their decisions
        self.channel.send_to(self.all_participants, NEW_COORDINATOR)
        num_of_others = len(self.all_participants) - 1
        process_did_vote = True
        while num_of_others > 0:
            num_of_others -= 1
            msg = self.channel.receive_from(self.all_participants, TIMEOUT * 2)
            if msg and msg[1] == NEW_COORDINATOR:
                pass
            else:
                process_did_vote = False
        if process_did_vote:
            smallest_process_id = min(self.all_participants)
            smallest_process_id = min(smallest_process_id, self.participant)
            self.logger.info(
                f"participant {self.participant} in state {self.state}: replacing crashed coordinator {self.coordinator} with {smallest_process_id}")
            self.all_participants.pop(smallest_process_id)
            self.coordinator = {str(smallest_process_id)}

    def run(self):
        # Wait for start of joint commit
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

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
                self.channel.send_to(self.coordinator, VOTE_ABORT)

            # If local decision is positive,
            # we are ready to proceed the joint commit
            else:
                assert decision == LOCAL_SUCCESS
                self.channel.send_to(self.coordinator, VOTE_COMMIT)
                self._enter_state('READY')

                pre_commit_message = self.channel.receive_from(self.coordinator, TIMEOUT)
                if not pre_commit_message or pre_commit_message[1] == GLOBAL_ABORT:
                    self.logger.info(
                        f"participant {self.participant} in state {self.state}: switching to state ABORT as the coordinator is not responding to VOTE_COMMIT message")
                    self._enter_state('ABORT')
                elif pre_commit_message[1] == PREPARE_COMMIT:
                    self._enter_state('PRECOMMIT')
                    self.channel.send_to(self.coordinator, READY_COMMIT)
                    decision = self.channel.receive_from(self.coordinator, TIMEOUT)[1]
                    if decision == GLOBAL_COMMIT:
                        self._enter_state('COMMIT')

        msg = self.channel.receive_from(self.all_participants, TIMEOUT * 2)
        if msg and msg[1] == NEW_COORDINATOR:
            smallest_process_id = min(self.all_participants)
            smallest_process_id = min(smallest_process_id, self.participant)
            self.logger.info(
                f"participant {self.participant} in state {self.state}: replacing crashed coordinator {self.coordinator} with {smallest_process_id}")
            self.all_participants.pop(smallest_process_id)
            self.coordinator = {smallest_process_id}
            # if this participant is now the new coordinator
            if next(iter(self.coordinator)) == self.participant:
                self.channel.send_to(self.all_participants, (STATE_CHANGE, self.state))
                num_of_others = len(self.all_participants) - 1
                participant_states = list()
                while num_of_others > 0:
                    num_of_others -= 1
                    msg = self.channel.receive_from(self.all_participants, TIMEOUT)
                    if msg and msg[1] in ['INIT', 'READY', 'ABORT', 'PRECOMMIT', 'COMMIT']:
                        participant_states.append(msg[1])
                if self.state == 'WAIT' and 'COMMIT' not in participant_states:
                    self._enter_state('ABORT')
                    self.channel.send_to(self.all_participants, GLOBAL_ABORT)
                elif self.state == 'PRECOMMIT' and ['ABORT', 'INIT'] not in participant_states:
                    self._enter_state('COMMIT')
                    self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
            else:
                msg = self.channel.receive_from(self.coordinator, TIMEOUT)
                if msg and msg[1][0] == STATE_CHANGE:
                    coordinator_state = msg[1][1]
                    if coordinator_state == 'WAIT' and self.state in ['INIT', 'WAIT']:
                        self._enter_state(coordinator_state)
                    elif coordinator_state == 'PRECOMMIT' and self.state in ['INIT', 'WAIT', 'PRECOMMIT']:
                        self._enter_state(coordinator_state)
                    elif self.state in ['INIT', 'WAIT', 'PRECOMMIT']:
                        self._enter_state(coordinator_state)
                    elif coordinator_state in ['COMMIT', 'ABORT']:
                        self._enter_state(coordinator_state)
                    self.channel.send_to(self.coordinator, self.state)
                    msg = self.channel.receive_from(self.coordinator, TIMEOUT)
                    if msg and msg[1] == GLOBAL_ABORT:
                        decision = GLOBAL_ABORT
                        self._enter_state('ABORT')
                    elif msg and msg[1] == GLOBAL_COMMIT:
                        decision = GLOBAL_COMMIT
                        self._enter_state('COMMIT')

        return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
