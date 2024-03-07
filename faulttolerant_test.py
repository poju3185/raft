import pytest
import requests
import time
from test_utils import Swarm

"""Fault-tolerant Test (Remember to launch enough nodes first)"""

NUM_NODES_ARRAY = [3, 5, 7]
PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 2.0


class ExtendedSwarm(Swarm):
    def __init__(self, program_file_path, num_nodes):
        super().__init__(program_file_path, num_nodes)
        self.failed_nodes = []

    def kill_node(self, node_id):
        node = self.nodes[node_id]
        node.terminate()
        self.failed_nodes.append(node_id)
        print(f"Node {node_id} killed.")

    def resolve_failures(self):
        for node_id in self.failed_nodes:
            self.start_node(node_id)
        self.failed_nodes.clear()
        print("All failed nodes have been resolved.")

@pytest.fixture(params=NUM_NODES_ARRAY)
def swarm(request):
    num_nodes = request.param
    swarm = ExtendedSwarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start()
    yield swarm
    swarm.clean()


def test_fault_tolerance(swarm: ExtendedSwarm):
    """
    Test the cluster's fault tolerance by simulating failures for up to N/2 - 1 nodes
    and verifying that the cluster still operates correctly.
    """
    num_nodes = len(swarm.nodes)
    max_failures = num_nodes // 2 - 1

    leader = swarm.get_leader()
    assert leader is not None, "Cluster should have a leader before failures"

    topic_name = "test_fault_tolerance"
    message = "Surviving failures"
    leader.create_topic(topic_name)
    leader.put_message(topic_name, message)

    # Induce failures up to N/2 - 1 nodes
    for _ in range(max_failures):
        swarm.kill_node(leader.id)
        time.sleep(ELECTION_TIMEOUT)
        new_leader = swarm.get_leader()
        assert new_leader is not None, "Cluster should elect a new leader after failure"
        response = new_leader.get_message(topic_name)
        assert response.status_code == 200, "Cluster should retrieve messages after failures"
        retrieved_message = response.json()["message"]
        assert retrieved_message == message, "Retrieved message should match the original message"

        leader = new_leader
    swarm.resolve_failures()
    time.sleep(ELECTION_TIMEOUT)
    final_leader = swarm.get_leader()
    assert final_leader is not None, "Cluster should have a leader after resolving failures"
    final_response = final_leader.get_message(topic_name)
    assert final_response.status_code == 200, "Cluster should retrieve messages after resolving failures"
    final_retrieved_message = final_response.json()["message"]
    assert final_retrieved_message == message, "Final retrieved message should match the original message"
    




