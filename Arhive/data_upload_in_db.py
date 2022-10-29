"""
Данные берутся из таблицы БД Statistics, формируется правильная ссылка и данные загружаются в
БД expert-support-bot
"""
import os
from src.get_data import (get_data_from_csv,
                          get_data_from_statistic)
from src.config import (LINK_SERVICE_URL,
                        text_storage,
                        db_credentials,
                        sys_pub_url,
                        root)
from src.data_types import Query, FastAnswer

data_from = "csv_file"

for data_from in ["csv_file"]:
    if data_from == "db_stat":
        """Загрузка данных из БД статистики:"""
        etalons_list, answers_list = get_data_from_statistic(LINK_SERVICE_URL, sys_pub_url,
                                                             db_credentials, test_mode=False)

        print("etalons list:\n", etalons_list)
        print("answers list:\n", answers_list)

        added_etalons = [Query(et.templateId, et.etalonText, et.etalonId,
                               et.SysID, et.moduleId, et.pubsList) for et in etalons_list]
        text_storage.add(added_etalons, "etalons")

        added_answers = [FastAnswer(x.templateId, x.templateText, x.pubId) for x in answers_list]
        text_storage.add(added_answers, "answers")

    elif data_from == "csv_file":
        DATA_PATH = os.path.join(root, "data", "queries_answers_9.csv")
        pubs = [9, 6, 8, 188, 220, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641]
        etalons_list, answers_list = get_data_from_csv(DATA_PATH, pubs)

        added_etalons = [Query(et.templateId, et.etalonText, et.etalonId,
                               et.SysID, et.moduleId, et.pubsList) for et in etalons_list]
        added_answers = [FastAnswer(x.templateId, x.templateText, x.pubId) for x in answers_list]

        fa_ids = [x.templateId for x in added_answers]

        text_storage.delete(fa_ids, "templateId", "answers")
        text_storage.delete(fa_ids, "templateId", "etalons")

        text_storage.add(added_etalons, "etalons")
        text_storage.add(added_answers, "answers")

