"""
https://pypi.org/project/autocorrect/
https://github.com/filyp/autocorrect



http://docs.deeppavlov.ai/en/master/features/models/spelling_correction.html
https://pypi.org/project/pyspellchecker/
"""
from autocorrect import Speller
from spellchecker import SpellChecker
from time import time

bad_words = ['раыазвивать', 'развиь', 'ыжить', "даити", "дакумэнты", "мыама"]

spell = Speller(lang='ru')
spell2 = SpellChecker(language='ru')

for w in bad_words:
    t = time()
    s_r = spell(w)
    print("autocorrect:", w, s_r, time() - t)
    s_r2 = spell2.correction(w)
    t = time()
    print("spellchecker:", w, s_r2, time() - t)