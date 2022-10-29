"""
Данные берутся из таблицы БД Statistics и формируется правильная ссылка
"""
import os
import pymssql
import requests
import asyncio
import functools
import logging
import pandas as pd
from uuid import uuid4
from src.data_types import (Etalon,
                            Answer,
                            Query,
                            ROW_FOR_ANSWERS,
                            FastAnswer)
from datetime import datetime

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)


def data_for_answer_create(link_service_url: str, pubs_urls: [], row_tuple: ROW_FOR_ANSWERS):
    """
    Create dict for answer_create function with rules in
    https://conf.action-media.ru/pages/viewpage.action?pageId=343781756
    service_link_url - URL for link make service

    sys_pub_url - dict, example:
    {SysID number: [(PubID number, sys Url)]}

    row from Stat DB example:
    {"SysID": 1,
    "GroupID": 82759599,
    "ModuleID": 85,
    "ID": 12626,
    "PubList": "6,8,9,11,17,22,24,69,186,188,192,193,220,223",
    "Topic": "договор",
    "Subtopic": "договор * услуга * заказчик * выгодный * оказывать; договор * услуга * заказчик * возмездный *
    выгодный * оказывать; договор * услуга * лицо * юр * оказывать; договор * услуга * оказание * заказчик * выгодный;
    договор оказания услуг без предоплаты; договор возмездного оказания услуг по управлению баром",
    "ParentModuleID": 118,
    "ParentID": 69753,
    "ParentPubList": "6,8,9,11,17,22,24,69,186,188,192,193,220,223",
    "ParentBegDate": "datetime.date(2019, 7, 1)",
    "ParentEndDate": "None",
    "ParentPubDivName": "Формы",
    "ChildBlockModuleID": 18,
    "ChildBlockID": 28960,
    "SortNum": 0,
    "ClusterGroup": 1,
    "Cluster": "договор оказания услуг выгодный заказчику",
    "DocName": "На что обратить внимание в договоре оказания услуг",
    "ShortAnswerText": "Когда заказчику нужно заказать услуги на выгодных для себя условиях, нужно заключить договор
    со специальными условиями. В договоре необходимо закрепить режим конфиденциальности, ответственность и обязанность
    исполнителя возместить заказчику потери из-за действий третьих лиц. Чтобы составить выгодный договор
    для исполнителя, рекомендуем использовать другой образец.",
    "IsNavigation": "False"}
    """

    def get_answ_urls(pubs_urls):
        pubs_answers = []
        for pub, url in pubs_urls:
            if row_tuple.ParentModuleID == 16 and row_tuple.ChildBlockModuleID in [86, 12]:
                module_id = row_tuple.ChildBlockModuleID
                document_id = row_tuple.ChildBlockID
            else:
                module_id = row_tuple.ParentModuleID
                document_id = row_tuple.ParentID

            answ_url_dict = {"sys_url": url,
                             "module_id": module_id,
                             "document_id": document_id,
                             }
            answer_text = "вот ссылка по вашему вопросу "
            pubs_answers.append({"pubID": int(pub), "templateId": int(row_tuple.ID),
                                 "answer_text": answer_text + str(answer_url_creater(link_service_url, **answ_url_dict))})
        return pubs_answers

    async def url_get_async():
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, functools.partial(get_answ_urls, pubs_urls))
        response = await future
        return response

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(url_get_async())
    return result

def answer_url_creater(link_service_url, **kwargs):
    """
    Создание правильной ссылки
    """
    query_url = "/".join([kwargs["sys_url"], str(kwargs["module_id"]), str(kwargs["document_id"])])
    data = {"linkUrl": query_url, "materialType": 0, "isActualEdition": True}
    response = requests.post(link_service_url, json=data)
    if response.status_code == 200:
        response_json = response.json()
        constructed_url = response_json["linkUrl"]
    else:
        logger.exception("for data {} return {} response.status_code is {}".format(str(data), str(response),
                                                                                   str(response.status_code)))
        constructed_url = "NO URL for ANSWER"
    return constructed_url


def rows_parsing(rows: [()]) -> []:
    """
    Parsing rows from DB and returning list of unique tuples with etalons and list of tuples with data for answers
    """
    etalons = []
    answers = []
    for row in rows:
        try:
            pubs = [int(pb) for pb in row["ParentPubList"].split(",") if pb != '']
            etalons.append(Query(row["ID"], row["Cluster"], str(uuid4()), row["SysID"],
                                 row["ModuleID"], str(pubs)))
            answers.append(ROW_FOR_ANSWERS(row["SysID"], row["ID"], row["ParentModuleID"], row["ParentID"],
                                           row["ChildBlockModuleID"], row["ChildBlockID"]))
        except ValueError as err:
            print(err, row, "\n", row["ParentPubList"])
    unique_etalons = list(set(etalons))
    unique_answers = list(set(answers))
    logger.info("Unique etalons tuples rows is {}".format(len(unique_etalons)))
    logger.info("Unique answers tuples rows is {}".format(len(unique_answers)))
    return unique_etalons, unique_answers


def tuples2etalons(etalons_list: [Query]) -> []:
    """"""
    etalons = [Etalon(templateId=et.templateId, etalonText=et.etalonText, etalonId=et.etalonId, SysID=et.SysID,
                      moduleId=et.moduleId, pubsList=et.pubsList) for et in etalons_list]
    logger.info("Etalons quantity from rows is {}".format(len(etalons)))
    return etalons


def tuples2answers(answers_list: [ROW_FOR_ANSWERS], **kwargs) -> []:
    """Converting rows of ROW into list of type Answer"""
    answers = []
    for num, row in enumerate(answers_list):
        answ_dicts = data_for_answer_create(kwargs["LINK_SERVICE_URL"], kwargs["pubs_urls"], row)
        answers += [Answer(templateId=answ_dict["templateId"],
                           templateText=answ_dict["answer_text"],
                           pubId=answ_dict["pubID"]) for answ_dict in answ_dicts]
        logger.info("{} from {}".format(num, len(answers_list)))
    return answers


class DataFromDB:
    """
    class for getting data from MS Server with specific SQL Query.
    """

    def __init__(self, **kwargs):
        conn = pymssql.connect(host=kwargs["server_host"],
                               port=1433,
                               user=kwargs["user_name"],
                               password=kwargs["password"],
                               database="master")
        self.cursor = conn.cursor(as_dict=True)

    def fetch_from_db(self, sys_id: int, date: str):
        """
        return data (rows) from MS SQL DB Statistic on date (usually today)
        """
        self.cursor.execute("SELECT * FROM StatisticsRAW.[search].FastAnswer_RBD  "
                            "WHERE SysID = {} AND (ParentBegDate IS NOT NULL AND ParentEndDate IS NULL "
                            "OR ParentEndDate > {})".format(sys_id, "'" + str(date) + "'"))
        logger.info("Total rows for SysID {} is {}".format(sys_id, self.cursor.rowcount))
        return self.cursor.fetchall()


def get_data_from_statistic(LINK_SERVICE_URL, sys_pub_url, db_credentials, test_mode=False):
    """
    Function, combine all entities for getting and creating etalons and answers from Statistic DB.
    If test_mode is True only 50 Answers will be returned.
    """
    today = datetime.today().strftime('%Y-%m-%d')

    for i in sys_pub_url:
        db_conn = DataFromDB(**db_credentials)
        rows = db_conn.fetch_from_db(i, today)
        etalons_tpl, answers_tpl = rows_parsing(rows)
        etalons_from_statistic = tuples2etalons(etalons_tpl)

        data_for_url = {
            "LINK_SERVICE_URL": LINK_SERVICE_URL,
            "pubs_urls": sys_pub_url[i]
        }
        if test_mode:
            answers_from_statistic = tuples2answers(answers_tpl[:50], **data_for_url)
        else:
            answers_from_statistic = tuples2answers(answers_tpl, **data_for_url)

    return etalons_from_statistic, answers_from_statistic


def get_data_from_csv(DATA_PATH, pubs):
    """Function, getting data from csv file with fields:

        templateId
        text
        templateText

        and returned list of Etalons and list of Answers
     """
    df = pd.read_csv(DATA_PATH, sep="\t")
    etln_dcts = df.to_dict(orient="records")
    etalons_from_csv = [Etalon(templateId=row["templateId"] + 99000000,
                               etalonText=row["text"],
                               etalonId=str(uuid4()),
                               SysID=999,
                               moduleId=99,
                               pubsList=str(pubs)) for row in etln_dcts]

    answers_tuples = []
    for pub in pubs:
        answers_tuples += [FastAnswer(row["templateId"] + 99000000,
                                      row["templateText"], pub) for row in etln_dcts]
    answers_from_csv = [Answer(templateId=x.templateId,
                               templateText=x.templateText,
                               pubId=x.pubId) for x in set(answers_tuples)]

    return etalons_from_csv, answers_from_csv


if __name__ == '__main__':
    from src.config import (LINK_SERVICE_URL,
                            db_credentials,
                            sys_pub_url,
                            text_storage,
                            root
                            )
    import time

    # оценка актуальных быстрых ответов:
    # text_storage.delete_all_from_table("answers")
    # text_storage.delete_all_from_table("etalons")
    t = time.time()
    etalons, answers = get_data_from_statistic(LINK_SERVICE_URL, sys_pub_url, db_credentials, test_mode=True)
    print("etalons from statistic:\n", etalons[:10], "\nanswers from statistic:\n", answers[:10],
          "\nlen nswers from statistic:", len(answers))
    print("working time:", time.time() - t)
    # text_storage.add(etalons, "etalons")
    # text_storage.add(answers, "answers")

    '''
    DATA_PATH = os.path.join(root, "data", "queries_answers_9.csv")
    pubs = [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641]
    etalons, answers = get_data_from_csv(DATA_PATH, pubs)
    print("etalons from csv:\n", etalons[:10], "\nnswers from csv:\n", answers[:10], 
          "\nlen nswers from csv:", len(answers))'''

