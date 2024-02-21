import dataclasses
from dataclasses import dataclass
from enum import Enum
import requests
from logg import debug_print
from timer import ResettableTimer
from threading import Lock
import threading
class Role(Enum):
    Follower = "Follower"
    Leader = "Leader"
    Candidate = "Candidate"

@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: int = None
    log: list = dataclasses.field(default_factory=list) # Not sure

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
        self.id = id
        self.state = PersistentState()
        self.role = Role.Follower
        self.peers = peers
        self.votes_received = set() # Count the votes received
        self.election_timer = ResettableTimer(self.run_election, 150, 300)
        self.election_timer.run()
        self.lock = Lock() # Not sure if we need a lock


    # handle serialized message
    def rpc_handler(self, sender_id, rpc_message_json):
        rpc_message = deserialize(rpc_message_json)
        if isinstance(rpc_message, VoteRequest):
            return self.handle_vote_request(sender_id, rpc_message)
    # Descide to vote or not based on the term or whether i have voted in this term
    def handle_vote_request(self, sender_id, vote_request):
        if vote_request.term < self.state.current_term:
            return serialize(VoteResponse(term=self.state.current_term, vote_granted=False))
        if (self.state.voted_for is None or self.state.voted_for == sender_id) and \
           self.is_log_up_to_date(vote_request.last_log_index, vote_request.last_log_term):
            self.state.voted_for = sender_id
            self.state.current_term = vote_request.term # Not sure to update the term to the vote.request.term or just self.term + 1 is enough?
            self.election_timer.reset()
            return serialize(VoteResponse(term=self.state.current_term, vote_granted=True))
        return serialize(VoteResponse(term=self.state.current_term, vote_granted=False))

    def is_log_up_to_date(self, last_log_index, last_log_term):
        # chekc if the log is up to date
        # have not finished
        return True
    
    def get_last_log_index(self):
        return len(self.log) - 1

    def get_last_log_term(self):
        return self.log[self.get_last_log_index()].term if len(self.log) else -1

    def run_election(self):
        self.state.current_term += 1
        self.role = Role.Candidate
        self.state.voted_for = self.id
        self.votes_received = {self.id}
        debug_print(f"Node {self.id} starting election for term {self.state.current_term}")
        self.broadcast_vote_requests()

    def broadcast_vote_requests(self):
        vote_request = serialize(VoteRequest(
            term=self.state.current_term,
            candidate_id=self.id,
            last_log_index=len(self.state.log) - 1,
            last_log_term=self.state.log[-1].term if self.state.log else 0,
        ))
        for peer in self.peers:
            self.send_vote_request(peer, vote_request)

    # Maybe we should use a thread pool to send vote request to all peers
    def send_vote_request(self, peer, vote_request):
        try:
            res = requests.post(f"{peer['ip']}:{peer['port']}/vote", json=vote_request)
            if res.ok:
                vote_response = deserialize(res.json())
                if vote_response.vote_granted:
                    self.votes_received.add(peer['id'])
                    if len(self.votes_received) > len(self.peers) // 2:
                        self.become_leader()
        except Exception as e:
            debug_print(f"Failed to send vote request to {peer['ip']}:{peer['port']} due to {e}")

    def become_leader(self):
        with self.lock:
            self.role = Role.Leader
            self.votes_received.clear()  
            debug_print(f"Node {self.id} is now the leader for term {self.state.current_term}.")

        self.send_heartbeats()

    def send_heartbeats(self):
        for peer in self.peers:
            self.send_heartbeat(peer)

    def send_heartbeat(self, peer):
        HEARTBEAT_INTERVAL = 0.5
        append_entries_rpc = serialize({
            'term': self.state.current_term,
            'leader_id': self.id,
        })

        try:
            response = requests.post(f"{peer['ip']}:{peer['port']}/append-entries", json=append_entries_rpc)
            if response.status_code == 200:
                pass
        except requests.exceptions.RequestException as e:
            debug_print(f"Failed to send heartbeat to {peer['id']} due to {e}")

        threading.Timer(HEARTBEAT_INTERVAL, self.send_heartbeats).start()



