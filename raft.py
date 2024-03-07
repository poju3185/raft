import dataclasses
from dataclasses import asdict, dataclass
from enum import Enum
import requests
from logg import debug_print
from timer import ResettableTimer
from threading import Lock
import threading
import time
import json


class Role(Enum):
    Follower = "Follower"
    Leader = "Leader"
    Candidate = "Candidate"


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: int | None = None


@dataclass
class LogEntry:
    action: str  # CREATE | APPEND | POP
    topic: str
    term: int
    message: str | None = None


@dataclass
class LogInfo:
    leader_id: int
    leader_term: int
    start_index: int
    commit_index: int
    logs: list[LogEntry]


@dataclass
class VoteRequest:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool


def serialize(rpc):
    return {"class": rpc.__class__.__qualname__, "dict": dataclasses.asdict(rpc)}


def deserialize(rpc_dict):
    return globals()[rpc_dict["class"]](**rpc_dict["dict"])


class Node:
    def __init__(self, id, peers):
        self.HEARTBEAT_INTERVAL = 0.1  # s
        self.election_timer = ResettableTimer(
            self.run_election, interval_lb=150, interval_ub=300
        )  # 150, 300  # ms
        self.election_timer.run()
        self.id: int = id
        self.topics: dict[str, list[str]] = {}
        self.state = PersistentState()
        self.role = Role.Follower
        self.peers: list[dict[str, int]] = peers
        self.log: list[LogEntry] = []
        self.log_confirmed: set[int] = set()
        self.votes_received: set[int] = set()  # Count the votes received
        self.lock = Lock()
        self.commit_index_condition = threading.Condition()
        self.commit_index = -1
        self.last_applied = -1
        # Only leader needs to maintain the table, the last same log between leader and follower
        self.next_index_table: dict[int, int] = {peer["id"]: -1 for peer in peers}
        self.match_index_table: dict[int, int] = {peer["id"]: -1 for peer in peers}

    # handle serialized message
    def rpc_handler(self, sender_id, rpc_message_json):
        rpc_message = deserialize(rpc_message_json)
        if isinstance(rpc_message, VoteRequest):
            with self.lock:
                return self.handle_vote_request(sender_id, rpc_message)

    # Decide to vote or not based on the term or whether i have voted in this term
    def handle_vote_request(self, sender_id, vote_request: VoteRequest):
        if vote_request.term < self.state.current_term:
            return serialize(
                VoteResponse(term=self.state.current_term, vote_granted=False)
            )

        # If the vote term is greater than the current term, or the term is equal to the current term and the node has not yet voted or already voted for the sender
        if (vote_request.term > self.state.current_term) or (
            self.state.voted_for is None
        ):
            if self.is_log_up_to_date(
                vote_request.last_log_index, vote_request.last_log_term
            ):
                self.role = Role.Follower
                self.state = PersistentState(
                    current_term=vote_request.term, voted_for=sender_id
                )
                self.election_timer.reset()
                return serialize(
                    VoteResponse(term=self.state.current_term, vote_granted=True)
                )

        return serialize(VoteResponse(term=self.state.current_term, vote_granted=False))

    def is_log_up_to_date(self, last_log_index, last_log_term):
        local_last_index = self.get_last_log_index()
        local_last_term = self.get_last_log_term()
        if last_log_term < local_last_term:
            return False
        if last_log_term == local_last_term and last_log_index < local_last_index:
            return False
        return True

    def get_last_log_index(self) -> int:
        return len(self.log) - 1

    def get_last_log_term(self) -> int:
        return self.log[self.get_last_log_index()].term if len(self.log) else -1

    def run_election(self):
        self.election_timer.reset()
        if self.role == Role.Leader:
            return
        with self.lock:
            self.state.current_term += 1
            self.role = Role.Candidate
            self.state.voted_for = self.id
            self.votes_received = {self.id}
        debug_print(
            f"Node {self.id} starting election for term {self.state.current_term}, timer interval {self.election_timer.timer.interval}"
        )
        self.broadcast_vote_requests()

    def broadcast_vote_requests(self):
        vote_request = serialize(
            VoteRequest(
                term=self.state.current_term,
                candidate_id=self.id,
                last_log_index=len(self.log) - 1,
                last_log_term=self.log[-1].term if self.log else 0,
            )
        )
        for peer in self.peers:
            self.send_vote_request(peer, vote_request)
        if (
            len(self.votes_received)
            > (len(self.peers) + 1) // 2  # include the node itself
            and self.role != Role.Leader
        ):
            self.become_leader()

    def send_vote_request(self, peer, vote_request):
        try:
            response = requests.post(
                f"http://{peer['ip']}:{peer['port']}/request-vote/{self.id}",
                json=vote_request,
                timeout=2,
            )
            response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx
            vote_response = deserialize(response.json())
            if not isinstance(vote_response, VoteResponse):
                raise TypeError("vote_response is not a VoteResponse object")
            debug_print(f"=====================\n{vote_response}\n=================")
            if vote_response.vote_granted:
                self.votes_received.add(peer["id"])
                if (
                    len(self.votes_received)
                    > (len(self.peers) + 1) // 2  # include the node itself
                    and self.role != Role.Leader
                ):
                    self.become_leader()
            else:
                debug_print("vote rejected")

        except Exception as e:
            pass
            # debug_print(
            #     f"Failed to send vote request to {peer["ip"]}:{peer["port"]} due to {e}"
            # )

    def become_leader(self):
        if self.role == Role.Leader:
            return

        with self.lock:
            self.role = Role.Leader
            self.votes_received.clear()
            self.state.voted_for = None
            self.election_timer.stop()
            self.next_index_table = {
                peer["id"]: self.get_last_log_index() for peer in self.peers
            }
            self.match_index_table = {peer["id"]: -1 for peer in self.peers}
            self.start_append_entry_loop()
        debug_print(
            f"Node {self.id} is now the leader for term {self.state.current_term}."
        )

    def start_append_entry_loop(self):
        self.heartbeat_thread = threading.Thread(target=self.send_append_entries_loop)
        self.heartbeat_thread.start()

    def send_append_entries_loop(self):
        while self.role == Role.Leader:
            self.update_commit_index()
            self.apply_to_state_machine(self.commit_index)
            self.send_append_entries()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def send_append_entries(self):
        for peer in self.peers:
            threading.Thread(target=self.send_append_entry, args=(peer,)).start()

    def send_append_entry(self, peer: dict):
        """
        The leader send a list of LogEntry to the followers
        """
        start_index = self.next_index_table[peer["id"]]
        if start_index != -1:
            data_to_sent = LogInfo(
                leader_id=self.id,
                leader_term=self.state.current_term,
                start_index=start_index,
                commit_index=self.commit_index,
                logs=self.log[start_index:],
            )
        else:  # start_index == -1 means follower's log is completely different from leader's, leader will just send the entire log
            data_to_sent = LogInfo(
                leader_id=self.id,
                leader_term=self.state.current_term,
                start_index=start_index,
                commit_index=self.commit_index,
                logs=self.log,
            )
        data_to_sent = json.dumps(asdict(data_to_sent))

        try:
            response = requests.post(
                f"http://{peer['ip']}:{peer['port']}/append_entry", json=data_to_sent
            )
            response.raise_for_status()
            data = response.json()
            self.next_index_table[peer["id"]] = data["last_index"]
            if data["accept"]:
                self.match_index_table[peer["id"]] = data["last_index"]

            debug_print(self.next_index_table)

        except requests.exceptions.RequestException as e:
            pass
            # debug_print(f"Failed to send heartbeat to {peer['ip']} due to {e}")

    def handle_append_entry(self, unserialized_data) -> dict:
        log_info_dict = json.loads(unserialized_data)
        leader_id = log_info_dict["leader_id"]
        leader_term = log_info_dict["leader_term"]
        start_index = log_info_dict["start_index"]
        commit_index = log_info_dict["commit_index"]
        logs = [LogEntry(**entry) for entry in log_info_dict["logs"]]
        debug_print(f"Received heart beat from {leader_id}, term {leader_term}")
        if leader_term < self.state.current_term:
            debug_print("term greater than leader")
            # if local term is greater than leader, don't reset the timer, wait for the next election
            return {"accept": False, "last_index": self.get_last_log_index()}
        self.election_timer.reset()
        debug_print("term <= leader")
        self.role = Role.Follower
        self.state = PersistentState(voted_for=None, current_term=leader_term)

        if start_index <= 0:  # Leader is sending all its log, just apply it
            self.log = logs
            self.apply_to_state_machine(commit_index)
            return {"accept": True, "last_index": self.get_last_log_index()}
        term = logs[0].term

        # Follower's log is equal to or longer than leader, just apply the remaining
        if (
            self.get_last_log_index() >= start_index
            and self.log[start_index].term == term
        ):
            self.log = self.log[:start_index] + logs
            self.apply_to_state_machine(commit_index)
            return {"accept": True, "last_index": self.get_last_log_index()}

        # Local has different log from leader,
        else:
            return {
                "accept": False,
                "last_index": min(start_index - 1, self.get_last_log_index()),
            }

    def update_commit_index(self):
        match_index_sorted = sorted(
            list(self.match_index_table.values())
            + [self.get_last_log_index()],  # Including self
            reverse=True,
        )
        majority_index = match_index_sorted[len(match_index_sorted) // 2]
        if majority_index < 0:
            return

        if majority_index > self.commit_index:
            # self.commit_index = majority_index
            with self.commit_index_condition:
                self.commit_index = majority_index
                self.commit_index_condition.notify_all()

    def apply_to_state_machine(self, commit_index: int):
        while self.last_applied < commit_index:
            with self.lock:
                try:
                    self.last_applied += 1
                    log = self.log[self.last_applied]
                    if log.action == "CREATE":
                        self.topics[log.topic] = []
                    elif log.action == "APPEND" and log.message:
                        self.topics[log.topic].append(log.message)
                    elif log.action == "POP":
                        self.topics[log.topic].pop(0)
                    else:
                        raise Exception("Unknown operation")

                except Exception as e:
                    debug_print(e)
    def commit_and_apply(self):
        self.update_commit_index()
        self.apply_to_state_machine(self.commit_index)

    def wait_until_commit(self, commit_index):
        with self.commit_index_condition:
            while self.commit_index < commit_index:
                self.commit_index_condition.wait()
