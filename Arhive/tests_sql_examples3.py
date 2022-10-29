# https://medium.com/@geoffreykoh/fun-with-fixtures-for-database-applications-8253eaf1a6d
# Standard imports
import requests
import sqlite3

# Third party imports
import pytest


@pytest.fixture
def setup_database():
    """ Fixture to set up the in-memory database with test data """
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE stocks
        (date text, trans text, symbol text, qty real, price real)''')
    yield conn


@pytest.fixture
def setup_test_data1(setup_database):
    cursor = setup_database
    sample_data = [
        ('2020-01-01', 'BUY', 'IBM', 1000, 45.0),
        ('2020-01-01', 'SELL', 'GOOG', 40, 123.0),
    ]
    cursor.executemany('INSERT INTO stocks VALUES(?, ?, ?, ?, ?)', sample_data)
    yield cursor


@pytest.fixture
def setup_test_data2(setup_database):
    cursor = setup_database
    sample_data = [
        ('2020-01-01', 'SELL', 'TESLA', 400, 233.0),
        ('2020-01-01', 'SELL', 'MSFT', 140, 980.0),
        ('2020-02-01', 'BUY', 'AMZN', 3000, 1200.0),
    ]
    cursor.executemany('INSERT INTO stocks VALUES(?, ?, ?, ?, ?)', sample_data)
    yield cursor


def test_with_sample_data2(setup_test_data2):
    # Test to make sure that there are 3 items in the database
    cursor = setup_test_data2
    assert len(list(cursor.execute('SELECT * FROM stocks'))) == 3
