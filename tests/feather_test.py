import os
import pandas as pd

PATH = "/home/alexey/Data/Yandex.Disk/data/preanswer-expsupp"

df = pd.read_feather(os.path.join(PATH, "Быстрые_ответы"))
print(df)
