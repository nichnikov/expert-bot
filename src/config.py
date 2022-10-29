import os
import json
import logging
from src.utils import get_project_root
from src.texts_storage import TextsStorage
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
root = get_project_root()

logging.basicConfig(
    # https://stackoverflow.com/questions/3220284/how-to-customize-the-time-format-for-python-logging
     # filename=os.path.join(root, "data", "HISTORYlistener.log"),
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',)

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)

with open(os.path.join(root, "data", "config.json")) as json_config_file:
    config = json.load(json_config_file)

VOCABULARY_SIZE = 33000
SHARD_SIZE = 50000

# VOCABULARY_SIZE = config["vocabulary_size"]
DB_PATH = os.path.join(root, "data", "queries.db")

load_dotenv(os.path.join(root, "data", ".env"))
ANSWER_URL = os.getenv("ANSWER_URL")
PUBS_BASE_URL = os.getenv("PUBS_URL")

"""инициализация класса, управляющего Базой данных:"""
text_storage = TextsStorage(db_path=DB_PATH)

"""урл сервиса формирования ссылок"""
LINK_SERVICE_URL = 'http://link-service-backend-link.prod.link.aservices.tech/api/v2/' \
                       'link-transformer_transform-cross-link'

"""доступ в БД статистики:"""
db_credentials = {
    "server_host": "statistics.sps.hq.amedia.tech",
    "user_name": "nichnikov_ro",
    "password": "220929SrGHJ#yu"
}

"""соответствие пабайди и урлов для формирования ссылок (для сервиса формирования ссылок)"""
sys_pub_url = {1: [(6, "https://www.1gl.ru/#/document"), (8, "https://usn.1gl.ru/#/document"),
                   (9, "https://vip.1gl.ru/#/document"), (188, "https://buh.action360.ru/#/document"),
                   (220, "https://plus.1gl.ru/#/document"), (24, "https://action.1gl.ru/#/document"),
                   (186, "https://demo.1gl.ru/#/document"), (215, "https://fns.1gl.ru/#/document"),
                   (69, "https://nornikel.1gl.ru/#/document")]}


try:
    """necessary for NN algorithm"""
    with open(os.path.join(root, "data", "labels_ids.json"), "r") as f:
        lb_ids_json = json.load(f)
except FileNotFoundError as err:
    logger.exception("labels_ids.json is not found {0}".format(err))
    lb_ids_json = {0: "0"}


LABELS_IDS = {int(i): lb_ids_json[i] for i in lb_ids_json}
