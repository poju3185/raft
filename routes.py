from dataclasses import asdict
from flask import Flask, request, jsonify
from logg import debug_print

from raft import LogEntry, Node, deserialize

app = Flask(__name__)

raft_node: Node = None  # type: ignore

# In-memory storage for topics and their messages
topics = {}


@app.route("/", methods=["GET"])
def home():
    return "Raft MQ final project"


@app.route("/request-vote/<id>", methods=["POST"])
def request_vote(id):
    rpc_message_json = request.json
    res = raft_node.rpc_handler(id, rpc_message_json)
    return jsonify(res)


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json
    if not data:
        return "", 400
    term = data.get("term")
    id = data.get("leader_id")
    raft_node.handle_heartbeat(term, id)
    return "", 200


@app.route("/topic", methods=["PUT"])
def create_topic():

    # data = request.json
    # if not data:
    #     return jsonify(success=False), 400  # Bad Request for missing request body
    # topic = data.get("topic")
    # if topic is None or not isinstance(topic, str):
    #     return jsonify(success=False), 400  # Bad Request for invalid input
    # if topic in topics:
    #     return jsonify(success=False), 409  # Conflict if topic already exists
    # topics[topic] = []
    return jsonify(success=True)


@app.route("/topic", methods=["GET"])
def list_topics():
    return jsonify(success=True, topics=list(topics.keys()))


@app.route("/message", methods=["PUT"])
def add_message():
    data = request.json
    if not data:
        return jsonify(success=False), 400  # Bad Request for missing request body
    topic = data.get("topic")
    message = data.get("message")
    if topic is None or message is None:
        return jsonify(success=False), 400  # Bad Request for missing topic or message
    if topic not in topics:
        return jsonify(success=False), 404  # Not Found if topic does not exist
    topics[topic].append(message)
    return jsonify(success=True)


@app.route("/message/<topic>", methods=["GET"])
def get_message(topic):
    if topic not in topics or not topics[topic]:
        return (
            jsonify(success=False),
            404,
        )  # Not Found if topic does not exist or is empty
    message = topics[topic].pop(0)  # Pop the first message
    return jsonify(success=True, message=message)


@app.route("/append_entry", methods=["POST"])
def confirm_log():
    unserialize_data = request.get_json()
    try:
        response = raft_node.handle_append_entry(unserialize_data)  # type: ignore
        return jsonify(response)
    except Exception as e:
        debug_print(e)
        return f"{e}", 500




@app.route("/status", methods=["GET"])
def status():
    # This is a simplified placeholder. Actual implementation will vary.
    return jsonify(
        role=raft_node.role.value,
        term=raft_node.state.current_term,
        log=raft_node.log,
        table=raft_node.commit_index_table,
    )


# For testing
@app.route("/leader", methods=["GET"])
def leader():
    # This is a simplified placeholder. Actual implementation will vary.
    raft_node.become_leader()
    return "ok", 200


# For testing
@app.route("/election", methods=["GET"])
def election():
    # This is a simplified placeholder. Actual implementation will vary.
    raft_node.run_election()
    return "ok", 200

