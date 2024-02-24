import dataclasses
from dataclasses import dataclass
from enum import Enum
import requests
from logg import debug_print
from timer import ResettableTimer
from threading import Lock
import threading
import time




class Role(Enum):
    Follower = "Follower"
    Leader = "Leader"
    Candidate = "Candidate"


# We don't need this, just use the class variable
@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: int | None = None
    # log: list = dataclasses.field(default_factory=list)  # Not sure


@dataclass
class LogEntry:
    message: str
    term: int


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
        self.id: int = id
        self.state = PersistentState()
        self.role = Role.Follower
        self.peers: list[dict[str, int]] = peers
        self.log: list[LogEntry] = []
        self.votes_received: set[int] = set()  # Count the votes received
        self.election_timer = ResettableTimer(self.run_election, interval_lb=5000, interval_ub=5150)  # 150, 300
        self.election_timer.run()
        self.lock = Lock()  # Not sure if we need a lock

    def print_time_up(self):
        debug_print(f"times up. timer interval {self.election_timer.timer.interval}")
    # handle serialized message
    def rpc_handler(self, sender_id, rpc_message_json):
        rpc_message = deserialize(rpc_message_json)
        if isinstance(rpc_message, VoteRequest):
            with self.lock:
                return self.handle_vote_request(sender_id, rpc_message)

    # Descide to vote or not based on the term or whether i have voted in this term
    def handle_vote_request(self, sender_id, vote_request: VoteRequest):
        if vote_request.term < self.state.current_term:
            return serialize(
                VoteResponse(term=self.state.current_term, vote_granted=False)
            )

        # If the vote trem is greater than the current term, or the term is equal to the current term and the node has not yet voted or already voted for the sender
        if (vote_request.term > self.state.current_term) or (self.state.voted_for is None):
            if self.is_log_up_to_date(
                vote_request.last_log_index, vote_request.last_log_term
            ):
                with self.lock:
                    self.role = Role.Follower
                    self.state.current_term = vote_request.term
                    self.state.voted_for = sender_id
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
        self.state.current_term += 1
        with self.lock:
            self.role = Role.Candidate
            self.state.voted_for = self.id
            self.votes_received = {self.id}
        debug_print(f"Node {self.id} starting election for term {self.state.current_term}, timer interval {self.election_timer.timer.interval}")
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

    # Maybe we should use a thread pool to send vote request to all peers
    def send_vote_request(self, peer, vote_request):
        try:
            response = requests.post(
                f"http://{peer['ip']}:{peer['port']}/request-vote/{self.id}", json=vote_request, timeout=2
            )
            response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx
            vote_response = deserialize(response.json())
            if not isinstance(vote_response, VoteResponse):
                raise TypeError("vote_response is not a VoteResponse object")
            debug_print(f"=====================\n{vote_response}\n=================")
            if vote_response.vote_granted:
                self.votes_received.add(peer["ip"])
                if (
                    len(self.votes_received) > (len(self.peers)+1) // 2  # include the node itself
                    and self.role != Role.Leader
                ):
                    self.become_leader()
            else:
                debug_print("vote rejected")
 
        except Exception as e:
            debug_print(
                f"Failed to send vote request to {peer["ip"]}:{peer["port"]} due to {e}"
            )

    # RPC exposed function
    def handle_heartbeat(self, term, leader_id):
        debug_print(f"Recieved heart beat from {leader_id}, term {term}")
        if term >= self.state.current_term:
            debug_print("term <= leader")
            self.election_timer.reset()
            with self.lock:
                self.role = Role.Follower
            self.state.voted_for = None
            self.state.current_term = term
      
        else:
            debug_print("term greater than leader")
        # if the leader's term is less than self term, don't reset the timer

    def become_leader(self):
        with self.lock:
            self.role = Role.Leader
            self.votes_received.clear()
            self.state.voted_for = None
            self.election_timer.stop()
            self.start_heartbeat_loop()
        debug_print(
            f"Node {self.id} is now the leader for term {self.state.current_term}."
        )


    def start_heartbeat_loop(self):
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats_loop)
        # self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def send_heartbeats_loop(self):
        HEARTBEAT_INTERVAL = 3
        while self.role ==  Role.Leader:
            self.send_heartbeats()
            time.sleep(HEARTBEAT_INTERVAL)

    def send_heartbeats(self):
        heartbeat = {
                "term": self.state.current_term,
                "leader_id": self.id,
            }
        for peer in self.peers:
            threading.Thread(
                target=self.send_heartbeat, args=(peer, heartbeat)
            ).start()

 

    def send_heartbeat(self, peer, heartbeat):
        try:
            response = requests.post(f"http://{peer['ip']}:{peer['port']}/heartbeat", json=heartbeat)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            debug_print(f"Failed to send heartbeat to {peer['ip']} due to {e}")
    





