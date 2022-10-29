# https://habr.com/ru/company/naumen/blog/463683/

import os
import re
import time
from spellchecker import SpellChecker
import pandas as pd
from src.config import root
from src.texts_processing import TextsTokenizer

spell = SpellChecker(language='ru', distance=1)

tdf = pd.read_csv(os.path.join(root, "data", "etalons.csv"))
print(list(tdf["etalonText"]))
all_words = " ".join(list(tdf["etalonText"])).lower()
all_words_ = re.sub(r"[^\w\n\s]", " ", all_words)
all_words_unique = list(set(all_words_.split()))
t1 = time.time()
spell.word_frequency.load_words(all_words_unique)
kn = spell.known(['уточненной', 'этики'])
print("kn:", kn)

print("word_frequency.load_words time:", time.time() - t1)
print(len(all_words_unique))

for tx in list(tdf["etalonText"]):
     pass

tx = "Добрый день! вопрос по выплате суточных. Сотрудник выехал в командировку в воскресенье и прибыл " \
     "туда в понедельник. Суточные за воскресенье надо выплачивать ?"


tx = "Добрый день! вопрос по выплате суточных. Сотрудник выехал в командировку в воскресенье и прибылf " \
     "туда в понедельникg. Суточныеw за dвоскресеньеs надо wвыплачиватьs ?"


tknz = TextsTokenizer()
tkns = tknz([tx])
print(tkns)



spell.word_frequency.load_words(["мобилизация", "прибыль", "воскресенье", "в", "командировка", "суточные", "выплачивать"])
checking_world = spell.correction("мобилизацияф")
print(checking_world)

t = time.time()
print([spell.correction(w) for w in tx.split()])
print(time.time() - t)