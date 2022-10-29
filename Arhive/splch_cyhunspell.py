# https://pypi.org/project/cyhunspell/
# https://code.google.com/archive/p/hunspell-ru/
import os
from src.config import root
import time
from hunspell import Hunspell

h = Hunspell("ru_RU", hunspell_data_dir=os.path.join(root, "data", "dicts"))

tx = "Добрый день! вопрос по выплате суточных. Сотрудник выехал в командировку в воскресенье и прибыл " \
     "туда в понедельник. Суточные за воскресенье надо выплачивать ?"

tx = "абракадабра dпонедельникgwd Добрый день! вопрос по выплате суточных. Сотрудник выехал в командировку в воскресенье и прибылf " \
     "туда в понедельникg. Суточныеw за dвоскресеньеs надо wвыплачиватьs"


class SpellChecker:
    def __init__(self):
        self.h = Hunspell("ru_RU", hunspell_data_dir=os.path.join(root, "data", "dicts"))

    def spell_checking(self, text: str) -> str:
        """"""
        checking_dict = self.h.bulk_suggest(tx.split())
        return " ".join([checking_dict[w][0] for w in checking_dict])

    def __call__(self, text):
        return self.spell_checking(text)


spell = SpellChecker()

t = time.time()
tx_ = spell(tx)
print(time.time() - t)
print(tx_)

"""
print([h.spell(w) for w in tx.split()])
print(tx.split())
print(h.bulk_suggest(tx.split()))
print(time.time() - t)"""
