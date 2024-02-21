from flask import Flask, request, jsonify

from raft import Node

app = Flask(__name__)

raft_node: Node = None # type: ignore

# In-memory storage for topics and their messages
topics = {}
# Placeholder for node status
node_status = {"role": "Follower", "term": 0}


@app.route("/request-vote/<id>", methods=["POST"])
def request_vote(id):
    rpc_message_json = request.json
    res = raft_node.rpc_handler(id, rpc_message_json)
    return jsonify(res)


@app.route("/topic", methods=["PUT"])
def create_topic():
    data = request.json
    if not data:
        return jsonify(success=False), 400  # Bad Request for missing request body
    topic = data.get("topic")
    if topic is None or not isinstance(topic, str):
        return jsonify(success=False), 400  # Bad Request for invalid input
    if topic in topics:
        return jsonify(success=False), 409  # Conflict if topic already exists
    topics[topic] = []
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


@app.route("/status", methods=["GET"])
def status():
    # This is a simplified placeholder. Actual implementation will vary.
    return jsonify(role=node_status["role"], term=node_status["term"])
