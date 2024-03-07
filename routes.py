from dataclasses import asdict
from flask import Flask, request, jsonify
from logg import debug_print

from raft import LogEntry, Node, deserialize

app = Flask(__name__)

raft_node: Node = None  # type: ignore


"""
Client endpoints
"""


@app.route("/", methods=["GET"])
def home():
    return "Raft MQ final project"


@app.route("/topic", methods=["PUT"])
def create_topic():
    data = request.json
    if not data:
        return jsonify(success=False), 400  # Bad Request for missing request body
    topic = data.get("topic")
    if topic is None or not isinstance(topic, str):
        return jsonify(success=False), 400  # Bad Request for invalid input
    raft_node.log.append(
        LogEntry(action="CREATE", topic=topic, term=raft_node.state.current_term)
    )
    if topic in raft_node.topics:
        return jsonify(success=False), 409  # Conflict if topic already exists
    raft_node.wait_until_commit(raft_node.get_last_log_index())
    return jsonify(success=True)


@app.route("/topic", methods=["GET"])
def list_topics():
    return jsonify(success=True, topics=list(raft_node.topics.keys()))


@app.route("/message", methods=["PUT"])
def add_message():
    data = request.json
    if not data:
        return jsonify(success=False), 400  # Bad Request for missing request body
    topic = data.get("topic")
    message = data.get("message")
    if topic is None or message is None:
        return jsonify(success=False), 400  # Bad Request for missing topic or message
    if topic not in raft_node.topics:
        return jsonify(success=False), 404  # Not Found if topic does not exist
    raft_node.log.append(
        LogEntry(
            action="APPEND",
            topic=topic,
            message=message,
            term=raft_node.state.current_term,
        )
    )
    raft_node.wait_until_commit(raft_node.get_last_log_index())
    return jsonify(success=True)


@app.route("/message/<topic>", methods=["GET"])
def get_message(topic):
    if topic not in raft_node.topics or not raft_node.topics[topic]:
        return (
            jsonify(success=False),
            404,
        )  # Not Found if topic does not exist or is empty
    message = raft_node.topics[topic][0]  # Don't pop the message yet, just read
    raft_node.log.append(
        LogEntry(
            action="POP",
            topic=topic,
            term=raft_node.state.current_term,
        )
    )
    raft_node.wait_until_commit(raft_node.get_last_log_index())
    return jsonify(success=True, message=message)


"""
Server endpoints
"""


@app.route("/request-vote/<id>", methods=["POST"])
def request_vote(id):
    rpc_message_json = request.json
    res = raft_node.rpc_handler(id, rpc_message_json)
    return jsonify(res)


@app.route("/append_entry", methods=["POST"])
def confirm_log():
    unserialize_data = request.get_json()
    try:
        response = raft_node.handle_append_entry(unserialize_data)  # type: ignore
        return jsonify(response)
    except Exception as e:
        debug_print(e)
        return f"{e}", 500

# For debugging
@app.route("/status", methods=["GET"])
def status():
    return jsonify(
        role=raft_node.role.value,
        term=raft_node.state.current_term,
        log=raft_node.log,
        next_table=raft_node.next_index_table,
        match_table=raft_node.match_index_table,
        commit_index=raft_node.commit_index,
        topics=raft_node.topics,
    )


# For testing
@app.route("/leader", methods=["GET"])
def leader():
    raft_node.become_leader()
    return "ok", 200


# For testing
@app.route("/election", methods=["GET"])
def election():
    raft_node.run_election()
    return "ok", 200
