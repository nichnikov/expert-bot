# https://fastapi.tiangolo.com/tutorial/testing/
# from fastapi import FastAPI
import os
import requests
import json
from src.data_types import Etalons, TemplateId, TemplateIds, Answers, Answer, FastAnswer, Stopword, StopWords
from src.utils import queries2etalon, get_project_root, answers_fill, stopwords_fill
from src.get_data import get_queries

root = get_project_root()

with open(os.path.join(root, "data", "config.json")) as json_config_file:
    config = json.load(json_config_file)

# pubs = config["pubs"]
# answers_num = config["answers_num"]

# проверка загрузки эталонов (из Монги)
url_etalons_add = "http://0.0.0.0:4002/api/etalons/add"
url_etalons_delete = "http://0.0.0.0:4002/api/etalons/delete"
url_answers_add = "http://0.0.0.0:4002/api/answers/add"
url_answers_delete = "http://0.0.0.0:4002/api/answers/delete"

answers_num = 5
pub_id = 9
pubs = [pub_id]

qrs = get_queries(pubs, answers_num)
template_ids = [TemplateId(x.templateId) for x in qrs]

qrs = queries2etalon(qrs)
print("answers number:", len(qrs))

jsn_data = Etalons(etalons=qrs, pubId=pub_id).json()


response = requests.post(url_etalons_add, data=jsn_data)
print(response.status_code)
print(response.json())

'''
"""
jsn_data = TemplateIds(templateIds=template_ids, pubId=pub_id).json()
response = requests.post(url_delete, data=jsn_data)
print(response.status_code)
print(response.json())
"""

# проверка загрузки и удаления ответов из csv файла
import pandas as pd

answers_df = pd.read_csv(os.path.join(root, "data", "test_answers.csv"), sep="\t")
answers_add_url = "http://0.0.0.0:4002/api/answers/add"
answers_delete_url = "http://0.0.0.0:4002/api/answers/delete"


answers = [Answer_(x["templateId"], x["templateText"], x["pubId"]) for x in answers_df.to_dict(orient="records")]


answers_data = answers_fill(answers)
print(answers_data)

answers_json = Answers(answers=answers_data).json()
response = requests.post(answers_add_url, data=answers_json)
print(response.status_code)
print(response.json())

template_ids = [TemplateId(x.templateId) for x in answers]
answers_template_json = TemplateIds(templateIds=template_ids, pubId=526).json()

# response = requests.post(answers_delete_url, data=answers_template_json)
# print(response.status_code)
# print(response.json())

# проверка загрузки и удаления стоп слов из csv файла
stopwords_add_url = "http://0.0.0.0:4002/api/stopwords/add"
stopwords_delete_url = "http://0.0.0.0:4002/api/stopwords/delete"

stopwords_df = pd.read_csv(os.path.join(root, "data", "test_stopwords.csv"), sep="\t")


stopwords = stopwords_fill(stopwords_df["stopwords"])
stopwords_json = StopWords(stopwords=stopwords).json()

response = requests.post(stopwords_add_url, data=stopwords_json)
print(response.status_code)
print(response.json())'''

"""
response = requests.post(stopwords_delete_url, data=stopwords_json)
print(response.status_code)
print(response.json())
"""

