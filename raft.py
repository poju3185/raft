import dataclasses
from dataclasses import dataclass
from enum import Enum

import requests

from logg import debug_print
from timer import ResettableTimer


class Role(Enum):
    Follower = "Follower"
    Leader = "Leader"
    Candidate = "Candidate"


@dataclass
class PersistentState:
    pass


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


def serialize(rpc):
    return {"class": rpc.__class__.__qualname__, "dict": dataclasses.asdict(rpc)}


def deserialize(rpc_dict):
    return globals()[rpc_dict["class"]](**rpc_dict["dict"])


class Node:
    def __init__(self, id, peers):
        self.id = id
        self.current_term = 0
        self.role = Role.Candidate
        self.peers = peers
        self.log = []
        self.election_timer = ResettableTimer(self.run_election)
        # self.election_timer.run()

    def rpc_handler(self, sender_id, rpc_message_json):
        debug_print("received rpc str", sender_id, rpc_message_json)
        rpc_message = deserialize(rpc_message_json)
        debug_print("received rpc", rpc_message)
        return {"thank you": "come again"}

    def run_election(self):
        if self.role == Role.Leader:
            return

        debug_print("starting election")
        self.broadcast_fn(
            serialize(
                VoteRequest(
                    self.current_term,
                    self.id,
                    self.get_last_log_index(),
                    self.get_last_log_term(),
                )
            )
        )

    def broadcast_fn(self, json_payload):
        for _ip, port in self.peers:
            res = requests.post(
                f"http://localhost:{port}/request-vote/{self.id}", json=json_payload
            )
            if res.ok:
                print(res.json())

    def get_last_log_index(self):
        return len(self.log) - 1

    def get_last_log_term(self):
        return self.log[self.get_last_log_index()].term if len(self.log) else -1
