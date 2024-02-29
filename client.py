import requests
from time import sleep

from raft import VoteRequest, serialize

vote_request = VoteRequest(0, 2, 3, 4)

res = requests.put("http://localhost:46781/topic", json={"topic": "food"})
res12 = requests.get("http://localhost:46781/topic", json={"topic": "food"})
# res2 = requests.put("http://localhost:46781/message", json={"topic": "food","message": "hio"})
# res3 = requests.put("http://localhost:46781/message", json={"topic": "food","message": "hio2"})
# res4 = requests.get("http://localhost:46781/message/food")
# res5 = requests.get("http://localhost:46781/message/food")
print(res.json())
print(res12.json())
# print(res3.json())
# print(res4.json())
# print(res5.json())
