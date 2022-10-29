"""
Данные берутся из csv файла и загружаются в БД expert-support-bot
"""

import os
import json
import requests
import pandas as pd
from uuid import uuid4
from src.config import (text_storage,
                        root)
'''
etalons_add_url = "http://0.0.0.0:4002/api/etalons/add"
etalons_del_url = "http://0.0.0.0:4002/api/etalons/delete"

answer_add_url = "http://0.0.0.0:4002/api/answers/add"
answer_del_url = "http://0.0.0.0:4002/api/answers/delete"

stopwords_add_url = "http://0.0.0.0:4002/api/stopwords/add"
stopwords_del_url = "http://0.0.0.0:4002/api/stopwords/delete"
'''
etalons_add_url = "http://srv01.nlp.dev.msk2.sl.amedia.tech:4005/api/etalons/add"
answer_add_url = "http://srv01.nlp.dev.msk2.sl.amedia.tech:4005/api/answers/add"

# etalons_add:
df = pd.read_csv(os.path.join(root, "data", "queries_answers_9.csv"), sep="\t")
etln_dcts_list = df.to_dict(orient="records")

pubs = [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641]
etalons = [{"templateId": row["templateId"] + 99000000,
            "etalonText": row["text"],
            "etalonId": str(str(uuid4())),
            "SysID": 999,
            "moduleId": 99,
            "pubsList": str(pubs)} for row in etln_dcts_list]

et_add_data = {"SysID": 1, "etalons": etalons[:300]}
response = requests.post(etalons_add_url, json=et_add_data)
print(response.status_code)

# etalons_del:
'''
et_del_data = {"templateIds": [99000001]}
response = requests.post(etalons_del_url, json=et_del_data)
print(response.status_code)
'''
# answer_add:
answers = []
for pubid in pubs:
    answers += [{"templateId": row["templateId"] + 99000000,
                 "templateText": row["templateText"],
                 "pubId": pubid} for row in etln_dcts_list]

asw_add_data = {"answers": answers[:100]}
response = requests.post(answer_add_url, json=asw_add_data)
print(response.status_code)


'''
asw_del_data = {"templateIds": [99000001]}
response = requests.post(answer_del_url, json=asw_del_data)
print(response.status_code)

stwrds_add_data = {"stopwords": ["тестовый", "субтестовый"]}
response = requests.post(stopwords_add_url, json=stwrds_add_data)
print(response.status_code)

stwrds_del_data = {"stopwords": ["тестовый"]}
response = requests.post(stopwords_del_url, json=stwrds_del_data)
print(response.status_code)
'''
