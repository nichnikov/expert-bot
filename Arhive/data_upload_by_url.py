"""
Данные берутся из таблицы БД Statistics, формируется правильная ссылка и данные загружаются в
БД expert-support-bot
"""
import os
import logging
import requests
from src.get_data import (get_data_from_csv,
                          get_data_from_statistic)
from src.config import (LINK_SERVICE_URL,
                        db_credentials,
                        sys_pub_url,
                        root)
from src.utils import chunks

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

etalons_add_url = "http://0.0.0.0:4002/api/etalons/add"
answer_add_url = "http://0.0.0.0:4002/api/answers/add"


# etalons_add_url = "http://srv01.nlp.dev.msk2.sl.amedia.tech:4005/api/etalons/add"
# answer_add_url = "http://srv01.nlp.dev.msk2.sl.amedia.tech:4005/api/answers/add"


def upload_data_from_statistic(**kwargs):
    """Загрузка данных из БД статистики:"""
    etalons_list, answers_list = get_data_from_statistic(kwargs["LINK_SERVICE_URL"], kwargs["sys_pub_url"],
                                                         kwargs["db_credentials"], test_mode=True)

    etalons_chunks = [x for x in chunks(etalons_list, 2000)]
    for etalons_chunk in etalons_chunks:
        etalons = [e.dict() for e in etalons_chunk]
        et_add_data = {"SysID": kwargs["SysID"], "etalons": etalons}
        response = requests.post(kwargs["etalons_add_url"], json=et_add_data)
        logger.info("sending {} etalons with status_code {}".format(str(len(etalons_chunk)),
                                                                    str(response.status_code)))

    answers_chunks = [x for x in chunks(answers_list, 2000)]
    for answers_chunk in answers_chunks:
        answers = [a.dict() for a in answers_chunk]
        asw_add_data = {"answers": answers}
        response = requests.post(kwargs["answer_add_url"], json=asw_add_data)
        logger.info("sending {} answers with status_code {}".format(str(len(answers_chunk)),
                                                                    str(response.status_code)))
    return 0


def upload_data_from_csv(**kwargs):
    """Загрузка данных из CSV файла:"""
    etalons_list, answers_list = get_data_from_csv(kwargs["DATA_PATH"], kwargs["pubs"])

    etalons = [e.dict() for e in etalons_list]
    et_add_data = {"SysID": kwargs["SysID"], "etalons": etalons}
    response = requests.post(etalons_add_url, json=et_add_data)
    logger.info("sending {} etalons with status_code {}".format(str(len(etalons)),
                                                                str(response.status_code)))

    answers = [a.dict() for a in answers_list]
    asw_add_data = {"answers": answers}
    response = requests.post(answer_add_url, json=asw_add_data)
    logger.info("sending {} etalons with status_code {}".format(str(len(answers)),
                                                                str(response.status_code)))
    return 0


if __name__ == "__main__":
    statistic_parameters = {"LINK_SERVICE_URL": LINK_SERVICE_URL,
                            "sys_pub_url": sys_pub_url,
                            "db_credentials": db_credentials,
                            "etalons_add_url": etalons_add_url,
                            "answer_add_url": answer_add_url,
                            "SysID": 1}

    upload_data_from_statistic(**statistic_parameters)

    csv_parameters = {"DATA_PATH": os.path.join(root, "data", "queries_answers_9.csv"),
                      "pubs": [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641],
                      "etalons_add_url": etalons_add_url,
                      "answer_add_url": answer_add_url,
                      "SysID": 1}

    upload_data_from_csv(**csv_parameters)
