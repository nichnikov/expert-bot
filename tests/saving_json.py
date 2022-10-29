import os
import json
from src.config import root

sys_pub_url = {1: [(6, "https://www.1gl.ru/#/document"), (8, "https://usn.1gl.ru/#/document"),
                   (9, "https://vip.1gl.ru/#/document"), (188, "https://buh.action360.ru/#/document"),
                   (220, "https://plus.1gl.ru/#/document"), (24, "https://action.1gl.ru/#/document"),
                   (186, "https://demo.1gl.ru/#/document"), (215, "https://fns.1gl.ru/#/document"),
                   (69, "https://nornikel.1gl.ru/#/document")]}

with open(os.path.join(root, "data", "sys_pub_url.json"), "w") as f:
    json.dump(sys_pub_url, f)