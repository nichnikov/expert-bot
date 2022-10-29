"""
Данные берутся из таблицы БД Statistics, формируется правильная ссылка и данные загружаются в
БД expert-support-bot
"""
import os
import pymssql
import requests
import logging
import pandas as pd
from collections import namedtuple
from uuid import uuid4
from src.data_types import (Etalon,
                            Answer,
                            Query,
                            FastAnswer)
from datetime import datetime
from src.config import (text_storage,
                        root)

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)

ROW = namedtuple("ROW", "SysID, ID, ParentModuleID, ParentID, ChildBlockModuleID, ChildBlockID")


def data_for_answer_create(link_service_url: str, sys_pub_url: {}, row_tuple: ROW):
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
    for item in sys_pub_url[row_tuple.SysID]:
        if row_tuple.ParentModuleID == 16 and row_tuple.ChildBlockModuleID in [86, 12]:
            module_id = row_tuple.ChildBlockModuleID
            document_id = row_tuple.ChildBlockID
        else:
            module_id = row_tuple.ParentModuleID
            document_id = row_tuple.ParentID

        answ_url_dict = {"sys_url": item[1],
                         "module_id": module_id,
                         "document_id": document_id,
                         }
        answer_text = "вот ссылка по вашему вопросу "
        pubs_answers.append({"pubID": int(item[0]), "templateId": int(row_tuple.ID),
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


LINK_SERVICE_URL = 'http://link-service-backend-link.prod.link.aservices.tech/api/v2/link-transformer_transform-cross-link'
server_host = "statistics.sps.hq.amedia.tech"
user_name = "nichnikov_ro"
passw = "220929SrGHJ#yu"
sys_pub_url = {1: [(6, "https://www.1gl.ru/#/document"), (8, "https://usn.1gl.ru/#/document"),
                   (9, "https://vip.1gl.ru/#/document"), (188, "https://buh.action360.ru/#/document"),
                   (220, "https://plus.1gl.ru/#/document"), (24, "https://action.1gl.ru/#/document"),
                   (186, "https://demo.1gl.ru/#/document"), (215, "https://fns.1gl.ru/#/document"),
                   (69, "https://nornikel.1gl.ru/#/document")]}

conn = pymssql.connect(host=server_host, port=1433, user=user_name, password=passw, database="master")
cursor = conn.cursor(as_dict=True)

# оценка актуальных быстрых ответов:
today = datetime.today().strftime('%Y-%m-%d')
text_storage.delete_all_from_table("answers")
text_storage.delete_all_from_table("etalons")

# TODO выборочное удаление из БД (удаление с условием)

for i in sys_pub_url:
    cursor.execute("SELECT * FROM StatisticsRAW.[search].FastAnswer_RBD  "
                   "WHERE SysID = {} AND (ParentBegDate IS NOT NULL AND ParentEndDate IS NULL "
                   "OR ParentEndDate > {})".format(i, "'" + str(today) + "'"))

    rows = cursor.fetchall()

    logger.info("Totat rows for SysID {} is {}".format(i, cursor.rowcount))

    etalons = []
    answers = []
    tuples_for_answers = []
    """create etalons:"""
    for row in rows:
        try:
            pubs = [int(pb) for pb in row["ParentPubList"].split(",") if pb != '']
            etalons.append(Etalon(
                templateId=row["ID"],
                etalonText=row["Cluster"],
                etalonId=str(uuid4()),
                SysID=row["SysID"],
                moduleId=row["ModuleID"],
                pubsList=str(pubs)))
        except ValueError as err:
            print(err, row, "\n", row["ParentPubList"])

        tuples_for_answers.append(ROW(row["SysID"],
                                      row["ID"],
                                      row["ParentModuleID"],
                                      row["ParentID"],
                                      row["ChildBlockModuleID"],
                                      row["ChildBlockID"]))

    logger.info("Etalons quantity from rows for SysId {} is {}".format(i, len(etalons)))
    unique_tuples_for_answers = list(set(tuples_for_answers))
    """create answers:"""
    for num, row_dct in enumerate(unique_tuples_for_answers):
        answ_dicts = data_for_answer_create(LINK_SERVICE_URL, sys_pub_url, row_dct)
        answers += [Answer(templateId=answ_dict["templateId"],
                           templateText=answ_dict["answer_text"],
                           pubId=answ_dict["pubID"]) for answ_dict in answ_dicts]
        logger.info("{} from {}".format(num, len(unique_tuples_for_answers)))

    logger.info("Answers quantity from rows for SysId {} is {}".format(i, len(answers)))

    added_etalons = [Query(et.templateId, et.etalonText, et.etalonId,
                           et.SysID, et.moduleId, et.pubsList) for et in etalons]
    added_answers = [FastAnswer(x.templateId, x.templateText, x.pubId) for x in answers]
    text_storage.add(added_answers, "answers")
    text_storage.add(added_etalons, "etalons")

df = pd.read_csv(os.path.join(root, "data", "queries_answers_9.csv"), sep="\t")

etln_dcts = df.to_dict(orient="records")

pubs = [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641]
etalons = [Etalon(templateId=row["templateId"] + 99000000,
                  etalonText=row["text"],
                  etalonId=str(uuid4()),
                  SysID=999,
                  moduleId=99,
                  pubsList=str(pubs)) for row in etln_dcts]

added_etalons = [Query(et.templateId, et.etalonText, et.etalonId,
                       et.SysID, et.moduleId, et.pubsList) for et in etalons]
text_storage.add(added_etalons, "etalons")

answers_tuples = []
for pub in pubs:
    answers_tuples += [FastAnswer(row["templateId"] + 99000000,
                                  row["templateText"], pub) for row in etln_dcts]

print("len answers_tuples:", len(answers_tuples))
print("len set answers_tuples:", len(set(answers_tuples)))
answers = [Answer(templateId=x.templateId,
                  templateText=x.templateText,
                  pubId=x.pubId) for x in set(answers_tuples)]
added_answers = [FastAnswer(x.templateId, x.templateText, x.pubId) for x in answers]
text_storage.add(added_answers, "answers")
