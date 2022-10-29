# https://stackoverflow.com/questions/67255653/how-to-set-up-and-tear-down-a-database-between-tests-in-fastapi


import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from main import app, get_db
from database import Base


SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


@pytest.fixture()
def test_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)


def test_get_todos(test_db):
    response = client.post("/todos/", json={"text": "some new todo"})
    data1 = response.json()
    response = client.post("/todos/", json={"text": "some even newer todo"})
    data2 = response.json()

    assert data1["user_id"] == data2["user_id"]

    response = client.get("/todos/")

    assert response.status_code == 200
    assert response.json() == [
        {"id": data1["id"], "user_id": data1["user_id"], "text": data1["text"]},
        {"id": data2["id"], "user_id": data2["user_id"], "text": data2["text"]},
    ]


def test_get_empty_todos_list(test_db):
    response = client.get("/todos/")

    assert response.status_code == 200
    assert response.json() == []