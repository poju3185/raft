import requests

from raft import VoteRequest, serialize

vote_request = VoteRequest(0, 2, 3, 4)

res = requests.post(
    "http://localhost:46781/request-vote/1234", json=serialize(vote_request)
)
if res.ok:
    print(res.json())
