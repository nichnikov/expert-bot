# https://codersathi.com/install-mysql-odbc-driver-in-ubuntu/
# https://onstartup.ru/biblioteki/unixodbc/
# https://stackoverflow.com/questions/38534154/linux-python3-cant-open-lib-sql-server
# Microsoft:
# https://learn.microsoft.com/ru-ru/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16#debian18
import pyodbc
from datetime import datetime

class Sql:
    def __init__(self, database, server="statistics.sps.hq.amedia.tech"):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server="+server+";"
                                   "Database="+database+";"
                                   "Trusted_Connection=yes;")
        self.query = "-- {}\n\n-- Made in Python".format(datetime.now()
                                                         .strftime("%d/%m/%Y"))

"{/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1}"
cnxn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=statistics.sps.hq.amedia.tech;'
                      'Port=1433;'
                      'DATABASE=StatisticsRAW.[search].FastAnswer_RBD;'
                      'UID=Another_Domain\\nichnikov_ro;'
                      'PWD=220929SrGHJ#yu; '
                      'Trusted connection=YES')
