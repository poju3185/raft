import json
import sys

import routes
from raft import Node
from routes import app


def parse_config_json(fp, idx):
    config_json = json.load(open(fp))

    my_ip, my_port = None, None
    peers: list[dict[str, int]] = []
    for i, address in enumerate(config_json["addresses"]):
        ip, port = address["ip"], str(address["port"])
        if i == idx:
            my_ip, my_port = ip, port
            my_port = int(my_port)
        else:
            peers.append({"id": i, "ip": ip, "port": int(port)})

    assert my_ip, my_port
    return my_ip, my_port, peers


if __name__ == "__main__":
    config_json_fp = sys.argv[1]
    config_json_idx = int(sys.argv[2])
    _my_ip, my_port, peers = parse_config_json(config_json_fp, config_json_idx)

    routes.raft_node = Node(config_json_idx, peers)
    app.run(debug=False, host="localhost", port=my_port, threaded=True)  # type: ignore
