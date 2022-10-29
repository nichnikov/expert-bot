# https://docs.sqlalchemy.org/en/14/dialects/mssql.html
"""
http://www.pymssql.org/en/latest/faq.html#cannot-connect-to-sql-server
"""
import os
import time
from datetime import datetime
import pymssql
from queue import Queue
import pyarrow as pa
import pyarrow.parquet as pq
import threading
import itertools as IT
import multiprocessing as mp


server_host = "statistics.sps.hq.amedia.tech"
user_name = "nichnikov_ro"
passw = "220929SrGHJ#yu"
# "StatisticsRAW.search"
conn = pymssql.connect(host=server_host, port=1433, user=user_name, password=passw, database="master")
print(conn)
cursor = conn.cursor()
print(cursor.execute("SELECT @@VERSION"))
print(cursor.fetchone()[0])
q = Queue()
cursor = conn.cursor(as_dict=True)
t = time.time()

# оценка актуальных быстрых ответов:
today = datetime.today().strftime('%Y-%m-%d')
print(str(today))
cursor.execute("SELECT * FROM StatisticsRAW.[search].FastAnswer_RBD  "
               "WHERE SysID = 1 AND (ParentBegDate IS NOT NULL AND ParentEndDate IS NULL "
               "OR ParentEndDate > {})".format("'"+str(today)+"'"))

rows = cursor.fetchall()
print("rows:", rows[:10])
parent_m_id = []
ms_ids = []
for r in rows:
    parent_m_id.append((r["ParentModuleID"], r["ParentID"]))
    ms_ids.append((r["ModuleID"], r["ID"]))

print("all parent ids:", len(parent_m_id))
print("unique parent ids:", len(set(parent_m_id)))
print("all module and ids:", len(parent_m_id))
print("unique module and ids:", len(set(parent_m_id)))

total = cursor.rowcount
print("total:", total)


'''
cursor.execute('SELECT * FROM StatisticsRAW.[search].FastAnswer_RBD')
#cursor.execute('SELECT * FROM StatisticsRAW.[search].FastAnswer_RBD')

t1 = time.time()
for row in cursor:
    q.put(row)
q.put("END")

print("results.append time:", time.time() - t1)

iterator = iter(q.get, 'END')

tables = []
for chunk in iter(lambda: list(IT.islice(iterator, 10000)), []):
    tables.append(pa.Table.from_pylist(chunk))

table = pa.concat_tables(tables, promote=True)
print("len tables:", len(tables))
del tables
print(table.shape)

FA_PATH = "/home/an/Data/Yandex.Disk/data/fast_answers"
pq.write_table(table, os.path.join(FA_PATH, "fa_all221010.feather"))
print("I've wrote")


# read testing:
tb = pq.read_table(os.path.join(FA_PATH, "fa_all221010.feather"))
tb_ls = tb.to_pylist()
print(tb_ls[:10])
'''