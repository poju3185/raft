import json
import sys

import routes
from raft import Node
from routes import app


def parse_config_json(fp, idx):
    config_json = json.load(open(fp))

    my_ip, my_port = None, None
    peers = []
    for i, address in enumerate(config_json["addresses"]):
        ip, port = address["ip"], str(address["port"])
        if i == idx:
            my_ip, my_port = ip, port
        else:
            peers.append((ip, port))

    assert my_ip, my_port
    return my_ip, my_port, peers


if __name__ == "__main__":
    config_json_fp = sys.argv[1]
    config_json_idx = int(sys.argv[2])
    _my_ip, my_port, peers = parse_config_json(config_json_fp, config_json_idx)

    routes.raft_node = Node(config_json_idx, peers)
    app.run(debug=True, host="localhost", port=my_port, threaded=True) # type: ignore
