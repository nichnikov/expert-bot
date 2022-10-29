# https://yandex.ru/dev/speller/doc/dg/reference/checkText.html
import requests

url = "https://speller.yandex.net/services/spellservice.json/checkTexts"
txs = ["синхрафазатрон", "разработка", "вперед"]
# response = requests.get(url=url, params={"text": tx})
tx = "интересное кино"
response = requests.post(url=url, data={"text": tx})
r = response.json()
print(r)
