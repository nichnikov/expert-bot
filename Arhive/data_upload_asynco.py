"""
to get data from url with asynco
https://stackoverflow.com/questions/22190403/how-could-i-use-requests-in-asyncio

Passing args, kwargs, to run_in_executor
https://stackoverflow.com/questions/53368203/passing-args-kwargs-to-run-in-executor
"""
import asyncio
import requests
import os
import requests
from src.get_data import (get_data_from_csv,
                          get_data_from_statistic)
from src.config import (LINK_SERVICE_URL,
                        db_credentials,
                        sys_pub_url,
                        root)
from src.utils import chunks
import functools


answer_add_url = "http://0.0.0.0:4002/api/answers/add"
etalons_add_url = "http://0.0.0.0:4002/api/etalons/add"


async def main():
    loop = asyncio.get_event_loop()
    future1 = loop.run_in_executor(None, functools.partial(get_data_from_statistic(LINK_SERVICE_URL,
                                                                                sys_pub_url, db_credentials),
                                                           ))
    response1 = await future1
    print(response1.text)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

"""Загрузка данных из БД статистики:"""
etalons_list, answers_list = get_data_from_statistic(LINK_SERVICE_URL, sys_pub_url,
                                                     db_credentials, test_mode=False)

etalons_chunks = [x for x in chunks(etalons_list, 2000)]
for etalons_chunk in etalons_chunks:
    etalons = [e.dict() for e in etalons_chunk]
    et_add_data = {"SysID": 1, "etalons": etalons}
    response = requests.post(etalons_add_url, json=et_add_data)
    print(response.status_code)

answers_chunks = [x for x in chunks(answers_list, 2000)]
for answers_chunk in answers_chunks:
    answers = [a.dict() for a in answers_chunk]
    asw_add_data = {"answers": answers}
    response = requests.post(answer_add_url, json=asw_add_data)
    print(response.status_code)
