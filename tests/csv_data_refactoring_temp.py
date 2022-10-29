import os
import pandas as pd
from src.config import (root,
                        text_storage)
from src.data_types import FastAnswer

df = pd.read_csv(os.path.join(root, "data", "all_qnswers221026.csv"), sep="\t")
df["templateText"] = df["templateText"].str.replace("вот", "Вот")
anws_dct = df.to_dict(orient="records")
# print(df.to_dict(orient="records"))

added_answers = [FastAnswer(x["templateId"], x["templateText"], x["pubId"]) for x in anws_dct]
fa_ids = [x.templateId for x in added_answers]
# print(fa_ids)

print(added_answers[:1000])
text_storage.delete(fa_ids, "templateId", "answers")
text_storage.add(added_answers, "answers")

