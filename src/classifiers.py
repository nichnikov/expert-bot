"""
классификатор KNeighborsClassifier в /home/an/Data/Yandex.Disk/dev/03-jira-tasks/aitk115-support-questions
"""

import os
import logging
import pickle
from src.config import root, SHARD_SIZE
from src.matrices import MatricesList
from src.embeddings import BowEmbedding
# from src.embeddings import  TransformerEmbedding
from src.data_types import (Etalon,
                            Query,
                            TemplateId,
                            IdVector)
from src.config import VOCABULARY_SIZE, text_storage
from src.data_types import SearchData
from src.utils import vectors_ids_fill

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def resulting_report(found_answers_ids: [], text_storage):
    """"""
    found_answer_texts = text_storage.classification(ids=[x[0] for x in found_answers_ids],
                                                     returned_column_name="templateText",
                                                     table_name="answers",
                                                     column_name="templateId")
    return list(zip([x[0] for x in found_answers_ids], [y[0] for y in found_answer_texts]))


class FastAnswerClassifier:
    """Объект для оперирования MatricesList и TextsStorage"""

    def __init__(self):
        self.text_storage = text_storage
        self.matrix_list = MatricesList(max_size=SHARD_SIZE)
        self.embedding = BowEmbedding(VOCABULARY_SIZE)  # функция, возвращающая [IdVector]

    @property
    def quantity(self) -> int:
        return self.matrix_list.quantity

    def vectors_maker(self, data: [Etalon]) -> [IdVector]:
        """"""
        vectors = self.embedding([item.etalonText for item in data])
        ids = [item.etalonId for item in data]
        return [IdVector(id=_id, vector=_vec) for _id, _vec in zip(ids, vectors)]

    def add_vectors(self, data: [Etalon]) -> None:
        """"""
        ids_vectors = self.vectors_maker(data)
        self.matrix_list.add(ids_vectors)

    def add(self, data: [Etalon]) -> None:
        """"""
        ids_vectors = self.vectors_maker(data)
        self.matrix_list.add(ids_vectors)
        etalons = [Query(et.templateId, et.etalonText, et.etalonId,
                         et.SysID, et.moduleId, et.pubsList) for et in data]
        self.text_storage.add(etalons, "etalons")

    def delete(self, data: []) -> None:
        """"""
        unique_ids = list(set(data))
        etalon_ids = self.text_storage.search(ids=unique_ids,
                                              returned_column_name="etalonId",
                                              column_name="templateId", table_name="etalons")
        self.matrix_list.delete([x[0] for x in etalon_ids])
        self.text_storage.delete(unique_ids, "templateId", "etalons")

    def delete_all(self) -> None:
        """"""
        text_storage.delete_all_from_table("answers")
        text_storage.delete_all_from_table("etalons")
        text_storage.delete_all_from_table("stopwords")
        self.matrix_list = MatricesList(max_size=SHARD_SIZE)

    def classification(self, id_vectors, score: float) -> []:
        """"""
        try:
            result_tuples = self.matrix_list.search(id_vectors, score)
            logger.info("classification result_tuples {}".format(str(result_tuples)))
        except Exception as err:
            logging.error("classification Error: {0}".format(err))
            result_tuples = []
        if result_tuples:
            searched_ids, found_ids, scores = zip(*result_tuples)
            found_answers_ids = self.text_storage.search(ids=found_ids, returned_column_name="templateId",
                                                         table_name="etalons",
                                                         column_name="etalonId")
            logger.info("classification found_answers_ids {}".format(str(found_answers_ids)))
            return [x[0] for x in found_answers_ids]  # возвращает айди ответов (сколько сможет найти)
        else:
            return []

'''
class TransformerKNeighborsClassifier:
    """Классификатор методом ближайших соседей"""

    def __init__(self, labels_ids):
        self.embedding = TransformerEmbedding()
        self.labels_ids = labels_ids  # {} соответствие класса модели айди ответа (предполагается, что они не совпадают)
        self.model = None

    def model_upload(self, model_name: str):
        with open(os.path.join(root, "models", model_name), "rb") as f:
            self.model = pickle.load(f)

    def classification(self, texts, score: float) -> []:
        """"""
        vectors = self.embedding(texts)
        labels = sorted([x for x in self.labels_ids])
        predict_labels = self.model.predict_proba(vectors)
        results = [lb for lb, sc in zip(labels, predict_labels[0]) if sc >= score]
        if results:
            return results  # [self.labels_ids[i] for i in results]
        else:
            return []

def nn_search(worker: TransformerKNeighborsClassifier, data: SearchData, score: float):
    """"""
    try:
        result = worker.classification([data.text], score)
        return result
    except Exception:
        logger.exception("Searching problem")
        return []
'''

def fa_search(worker: FastAnswerClassifier, text: str, score: float):
    """searching etalon by  incoming text"""
    try:
        tokens_list = set(worker.embedding.vectorizer.dictionary.token2id.keys())
        tokens = worker.embedding.tokenizer([text])
        """проверка наличия входящих токенов в словаре"""
        coeff = len(set(tokens[0]) | tokens_list) / (len(tokens_list))
        if coeff == 1.0:
            vectors = worker.embedding.vectorizer(tokens)
            id_vectors = vectors_ids_fill(vectors)
            result = worker.classification(id_vectors, score)
            return result
        else:
            return []
    except Exception:
        logger.exception("Searching problem")
        return []




'''
class CommonClassifier:
    """
    классификатор, каждому классу которого можно
    присвоить объект (например, другой классификатор)
    """

    def __init__(self):
        self.labeled_objects = {}  # сюда будут добавляться классы и соответствующие им объекты
        pass

    def add_labels(self, lbs_objs: [()]):
        """
        Добавление элемента класса и объекта ему соответствующего
        """
        for lb, obj in lbs_objs:
            self.labeled_objects[lb] = obj

    def delete_labels(self, lbs: []):
        """
        Удаление элемента класса и объекта ему соответствующего
        """
        for lb in lbs:
            del self.labeled_objects[lb]


class Element:
    """
    Elements for Pipline
    """

    def __init__(self):
        pass


class Pipeline:
    """
    Класс - конвейер последовательных классификаторов
    """

    def __init__(self):
        self.pipeline = []

    def add_element(self, element: Element):
        """
        Добавление элемента в конвейер
        """
        self.pipeline.append(element)

    def delete_element(self):
        """
        Удаление элемента из конвейера
        """
        pass

    def run(self, texts: [str]):
        """
        Run pipline
        """
        for el in self.pipeline:
            results = el(texts)
            if results != []:
                return results
        return []
'''
