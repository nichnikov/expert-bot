"""
Данные берутся из таблицы БД Statistics и формируется правильная ссылка
"""
import os
import pymssql
import requests
import logging
import pandas as pd
from uuid import uuid4
from src.data_types import (Etalon,
                            Answer,
                            Query,
                            ROW_FOR_ANSWERS,
                            ROW,
                            FastAnswer)
from src.utils import chunks
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
        answer_text = "Вот ссылка по вашему вопросу "
        pubs_answers.append({"pubID": int(pub), "templateId": int(row_tuple.ID),
                             "answer_text": answer_text + str(answer_url_creater(link_service_url, **answ_url_dict))})
    return pubs_answers


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
        self.rows = []

    def fetch_from_db(self, sys_id: int, date: str):
        """
        return data (rows) from MS SQL DB Statistic on date (usually today)
        """
        self.cursor.execute("SELECT * FROM StatisticsRAW.[search].FastAnswer_RBD  "
                            "WHERE SysID = {} AND (ParentBegDate IS NOT NULL AND ParentEndDate IS NULL "
                            "OR ParentEndDate > {})".format(sys_id, "'" + str(date) + "'"))
        logger.info("Total rows for SysID {} is {}".format(sys_id, self.cursor.rowcount))
        return self.cursor.fetchall()

    def get_rows(self, sys_id: int, date: str) -> []:
        """
        Parsing rows from DB and returning list of unique tuples with etalons and list of tuples with data for answers
        """
        data_from_db = self.fetch_from_db(sys_id, date)
        for row in data_from_db:
            try:
                parent_pub_list = [int(pb) for pb in row["ParentPubList"].split(",") if pb != '']
                self.rows.append(ROW(row["SysID"], row["ID"], row["Cluster"], row["ParentModuleID"],
                                     row["ParentID"], parent_pub_list, row["ChildBlockModuleID"],
                                     row["ChildBlockID"], row["ModuleID"]))
            except ValueError as err:
                logger.exception("Parsing {} with row: {}".format(err, str(row)))
        logger.info("Unique etalons tuples rows quantity is {} for SysID {}".format(len(self.rows), str(sys_id)))
        return 0


def get_answers_from_statistic(LINK_SERVICE_URL, rows_from_db: [ROW], pubs_urls: [], test_mode=False):
    """
    If test_mode is True only 10 Answers will be returned.
    """
    answers_tpl = list(set([ROW_FOR_ANSWERS(x.SysID, x.ID, x.ParentModuleID, x.ParentID,
                                            x.ChildBlockModuleID, x.ChildBlockID) for x in rows_from_db]))
    data_for_url = {
        "LINK_SERVICE_URL": LINK_SERVICE_URL,
        "pubs_urls": pubs_urls
    }
    if test_mode:
        return tuples2answers(answers_tpl[:10], **data_for_url)
    else:
        return tuples2answers(answers_tpl, **data_for_url)


def get_etalons_from_statistic(rows_from_db: [ROW], test_mode=False):
    """
    Function, combine all entities for getting and creating etalons and answers from Statistic DB.
    If test_mode is True only 50 Answers will be returned.
    """
    etalons_tpl = [Query(row.ID, row.Cluster, str(uuid4()), row.SysID,
                         row.ModuleID, str(row.ParentPubList)) for row in rows_from_db]
    if test_mode:
        return tuples2etalons(etalons_tpl[:5000])
    else:
        return tuples2etalons(etalons_tpl)


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


def upload_data_from_statistic(**kwargs):
    """Загрузка данных из БД статистики:"""
    today = datetime.today().strftime('%Y-%m-%d')
    db_con = DataFromDB(**kwargs["db_credentials"])
    db_con.get_rows(kwargs["SysID"], today)
    etalons_list = get_etalons_from_statistic(db_con.rows, test_mode=kwargs["test_mode"])
    answers_list = get_answers_from_statistic(kwargs["LINK_SERVICE_URL"],
                                              db_con.rows,
                                              kwargs["pubs_urls"],
                                              test_mode=kwargs["test_mode"])

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
    response = requests.post(kwargs["etalons_add_url"], json=et_add_data)
    logger.info("sending {} etalons with status_code {}".format(str(len(etalons)),
                                                                str(response.status_code)))

    answers = [a.dict() for a in answers_list]
    asw_add_data = {"answers": answers}
    response = requests.post(kwargs["answer_add_url"], json=asw_add_data)
    logger.info("sending {} etalons with status_code {}".format(str(len(answers)),
                                                                str(response.status_code)))
    return 0


if __name__ == '__main__':
    from src.config import (LINK_SERVICE_URL,
                            db_credentials,
                            sys_pub_url,
                            root)

    from src.config import (LINK_SERVICE_URL,
                            db_credentials,
                            sys_pub_url,
                            text_storage,
                            root
                            )
    import time

    etalons_add_url = "http://0.0.0.0:4002/api/etalons/add"
    answer_add_url = "http://0.0.0.0:4002/api/answers/add"

    # оценка актуальных быстрых ответов:
    t = time.time()
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

    '''
    DATA_PATH = os.path.join(root, "data", "queries_answers_9.csv")
    pubs = [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641]
    etalons, answers = get_data_from_csv(DATA_PATH, pubs)
    print("etalons from csv:\n", etalons[:10], "answers from csv:\n", answers[:10])'''
