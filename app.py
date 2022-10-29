""""""
import logging
import os
import uvicorn
from fastapi import FastAPI

from src.classifiers import FastAnswerClassifier
# from src.classifiers import TransformerKNeighborsClassifier
from src.classifiers import fa_search
# from src.classifiers import nn_search
from src.config import (LINK_SERVICE_URL,
                        LABELS_IDS,
                        db_credentials,
                        sys_pub_url,
                        text_storage,
                        root
                        )
from src.data_types import (Query,
                            FastAnswer,
                            Etalons,
                            TemplateIds,
                            Answers,
                            Stopwords,
                            SearchData)
from src.texts_processing import TextsTokenizer
# from src.texts_processing import SpellChecker
from src.utils import (worker_fill,
                       resulting_report)
from src.get_data import (upload_data_from_csv,
                          upload_data_from_statistic)

os.environ["TOKENIZERS_PARALLELISM"] = "false"
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

app = FastAPI(title="ExpSuppBot")
tokenizer = TextsTokenizer()
# spell_checker = SpellChecker(os.path.join(root, "data", "dicts"))

# nn_worker = TransformerKNeighborsClassifier(LABELS_IDS)
# nn_worker.model_upload("negh_classifier.sav")
fa_worker = FastAnswerClassifier()

global worker
score = 0.98
cnt = text_storage.get_count("etalons")[0]
if cnt[0] != 0:
    et_data = text_storage.get_data_from_table("etalons")
    pubs_data = [Query(*x) for x in et_data]
    all_stopwords = text_storage.get_data_from_column("stopwords", "stopword")
    worker = worker_fill(fa_worker, pubs_data, [x[0] for x in all_stopwords])
else:
    worker = FastAnswerClassifier()
    all_stopwords = text_storage.get_data_from_column("stopwords", "stopword")
    worker.embedding.add_stopwords([x[0] for x in all_stopwords])

service_host = "0.0.0.0"
service_port = 4002
service_url = "".join(["http://", service_host, ":", str(service_port)])
etalons_add_url = service_url + "/api/etalons/add"
answer_add_url = service_url + "/api/answers/add"


@app.post("/api/etalons/add")
def etalons_add(data: Etalons):
    """adding data and deleting data from service"""
    added_etalons = [Query(et.templateId, et.etalonText, et.etalonId,
                           et.SysID, et.moduleId, et.pubsList) for et in data.etalons]
    worker.add(data=added_etalons)


@app.post("/api/etalons/delete")
def etalons_delete(data: TemplateIds):
    """удаление данных по TemplateId (ID быстрого ответа)"""
    worker.delete(data.templateIds)


@app.post("/api/answers/add")
def answers_add(data: Answers):
    """"""
    added_answers = [FastAnswer(x.templateId, x.templateText, x.pubId) for x in data.answers]
    text_storage.add(added_answers, "answers")


@app.post("/api/answers/delete")
def answers_delete(data: TemplateIds):
    """"""
    text_storage.delete(list(set(data.templateIds)), "templateId", "answers")


@app.post("/api/stopwords/add")
def stopwords_add(data: Stopwords):
    """"""
    data_add = [(x,) for x in data.stopwords]
    text_storage.add(data_add, "stopwords")
    # all_stopwords = text_storage.get_data_from_column("stopwords", "stopword")
    worker.embedding.add_stopwords(data.stopwords)


@app.post("/api/stopwords/delete")
def stopwords_delete(data: Stopwords):
    """"""
    data_del = [x for x in data.stopwords]
    text_storage.delete(data_del, "stopword", "stopwords")
    worker.embedding.del_stopwords(data_del)


@app.post("/api/search")
def search(data: SearchData):
    """searching etalon by  incoming text"""
    try:
        # spell_text = spell_checker(data.text)
        # logger.info("searched text with spellcheck: {}".format(str(spell_text)))
        # result = fa_search(worker, spell_text, score)
        logger.info("searched text without spellcheck: {}".format(str(data.text)))
        result = fa_search(worker, data.text, score)
        if result:
            return resulting_report(data.pubid, result, text_storage)
        else:
            # result = nn_search(nn_worker, data, 0.8)
            return resulting_report(data.pubid, result, text_storage)
    except Exception:
        logger.exception("Searching problem with text {} in pubid {}".format(str(data.text), str(data.pubid)))
        return {"templateId": 0, "templateText": ""}


@app.get("/api/delete_all/{item}")
def delete_all(item: str):
    """Delete from DB and from searching matrix."""
    if item == "delete_all":
        worker.delete_all()
    return "OK"


@app.get("/api/add_data_from_source/{item}")
def add_data_from_source(item: str):
    """"""
    if item == "add_from_csv":
        csv_parameters = {"DATA_PATH": os.path.join(root, "data", "queries_answers_9.csv"),
                          "pubs": [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641],
                          "etalons_add_url": etalons_add_url,
                          "answer_add_url": answer_add_url,
                          "SysID": 1}
        upload_data_from_csv(**csv_parameters)
        return "OK"
    elif item == "add_from_statistic":
        for i in sys_pub_url:
            statistic_parameters = {"LINK_SERVICE_URL": LINK_SERVICE_URL,
                                    "pubs_urls": sys_pub_url[i],
                                    "db_credentials": db_credentials,
                                    "etalons_add_url": etalons_add_url,
                                    "answer_add_url": answer_add_url,
                                    "SysID": i}
            upload_data_from_statistic(**statistic_parameters)
        return "OK"


if __name__ == "__main__":
    uvicorn.run(app, host=service_host, port=service_port)
