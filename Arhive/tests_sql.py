# https://smirnov-am.github.io/pytest-testing_database/
# https://medium.com/@geoffreykoh/fun-with-fixtures-for-database-applications-8253eaf1a6d
import pytest
import sqlite3

@pytest.fixture # 1
def session():
    connection = sqlite3.connect(':memory:') # 2
    db_session = connection.cursor()
    db_session.execute('''CREATE TABLE numbers
                          (number text, existing boolean)''') # 3
    db_session.execute('INSERT INTO numbers VALUES ("+3155512345", 1)') # 4
    connection.commit()
    yield db_session # 5
    connection.close()


@pytest.fixture
def session(): # 1
    connection = sqlite3.connect(':memory:')
    db_session = connection.cursor()
    yield db_session
    connection.close()


@pytest.fixture
def setup_db(session): # 2
    session.execute('''CREATE TABLE numbers
                          (number text, existing boolean)''')
    session.execute('INSERT INTO numbers VALUES ("+3155512345", 1)')
    session.connection.commit()


@pytest.mark.usefixtures("setup_db") # 1
def test_get(session):
    ...

@pytest.fixture
def cache(session): # 1
    return CacheService(session)

@pytest.mark.usefixtures("setup_db")
def test_get(cache): # 2
    existing = cache.get_status('+3155512345')
    assert existing

class CacheService:
    def __init__(self, session): # 1
        self.session = session # 2

    def get_status(self, number):
        self.session.execute('SELECT existing FROM numbers WHERE number=?', (number,))
        return self.session.fetchone()

    def save_status(self, number, existing):
        self.session.execute('INSERT INTO numbers VALUES (?, ?)', (number, existing))
        self.session.connection.commit()

    def generate_report(self):
        self.session.execute('SELECT COUNT(*) FROM numbers')
        count = self.session.fetchone()
        self.session.execute('SELECT COUNT(*) FROM numbers WHERE existing=1')
        count_existing = self.session.fetchone()
        return count_existing[0]/count[0]

"""
from unittest.mock import MagicMock

def test_get_mock():
    session = MagicMock() # 1
    executor = MagicMock()
    session.execute = executor
    cache = CacheService(session) # 2
    cache.get_status('+3155512345')
    executor.assert_called_once_with('SELECT existing FROM numbers WHERE number=?', ('+3155512345',)) # 3

"""

"""
@pytest.fixture # 1
def session():
    connection = sqlite3.connect(':memory:') # 2
    db_session = connection.cursor()
    db_session.execute('''CREATE TABLE "answers"
                        (templateId INTEGER, templateText TEXT, pubId int)''')
    db_session.execute('''CREATE TABLE "etalons"
                        ("templateId" INTEGER, "etalonText" TEXT, "etalonId" TEXT, pubId int, moduleId int, pubs int)''')
    db_session.execute('''CREATE TABLE "stopwords"(stopword TEXT)''')
    connection.commit()
    yield db_session
    connection.close()

"""

