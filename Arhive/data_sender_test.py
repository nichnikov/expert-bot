import os
import pandas as pd
import json
from collections import namedtuple
from src.data_types import (Query,
                            Etalon,
                            TemplateIds,
                            Answer)
from src.utils import queries2etalon

from src.config import root
from src.config import text_storage

df = pd.read_csv(os.path.join(root, "data", "etalons.csv"))

for y in df[:10].values:
    print(tuple(y))

print(Query._fields)
etalons = queries2etalon([Query(*x) for x in df[:10000].values], etalonId_gen=False)
print(etalons)

answers = [Answer(templateId=ans_id, templateText="тестовый ответ для вопроса {0}".format(ans_id))
           for ans_id in set(df["templateId"])]

text_storage.get_data_from_table("etalons")
text_storage.get_data_from_table("answers")
text_storage.add(etalons, "etalons", Etalon)
text_storage.add(answers, "answers", Answer)

df_nn = pd.read_csv(os.path.join(root, "data", "nn_etalons.csv"), sep='\t')
answers_nn = [Answer(templateId=y, templateText="тестовый ответ для вопроса {0}".format(ans_id))
              for y, ans_id in set(zip(df_nn["y"], df_nn["answer"]))]
text_storage.add(answers_nn, "answers", Answer)

labels_ids = {y: a for y, a in zip(df_nn["y"], df_nn["answer"])}
with open(os.path.join(root, "data", "labels_ids.json"), "w") as f:
    json.dump(labels_ids, f)
