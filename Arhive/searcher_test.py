import os
import time
import pandas as pd
import requests
from src.config import root

df = pd.read_csv(os.path.join(root, "data", "etalons.csv"))
print(list(df["etalonText"])[:100])

url = "http://0.0.0.0:4002/api/search"
for tx in list(df["etalonText"])[:200]:
    tx_ = " ".join([w + "s" for w in tx.split()])
    rsp = {"pubid": 6, "text": tx_}
    print(tx_)
    t = time.time()
    response = requests.post(url, json=rsp)
    wt = time.time() - t
    print(response.json(), "working time:", wt)