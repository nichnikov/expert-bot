# https://fastapi.tiangolo.com/tutorial/testing/
# from fastapi import FastAPI
import os
import json
from fastapi.testclient import TestClient
from app_working import app
from src.data_types import Etalons
from src.utils import queries2etalon
from src.get_data import get_queries

client = TestClient(app)


def test_app():
    """"""
    answers_num = 5
    qrs = get_queries([6], answers_num)
    etns = queries2etalon(qrs)
    test_data = Etalons(etalons=etns, pubId=6, operation="add").json()
    response = client.post("/api/etalons", json=test_data)
    assert response.status_code == 422
