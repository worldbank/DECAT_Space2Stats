from unittest.mock import patch

import pytest
from pytest_postgresql.janitor import DatabaseJanitor

from fastapi.testclient import TestClient


aoi = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-74.1, 40.6],
                [-73.9, 40.6],
                [-73.9, 40.8],
                [-74.1, 40.8],
                [-74.1, 40.6],
            ]
        ],
    },
    "properties": {},
}

@pytest.fixture(scope="session")
def database(postgresql_proc):
    """Fake Database."""
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname="testdb",
        version=postgresql_proc.version,
        password="password",
    ) as jan:
        yield jan


@pytest.fixture(autouse=True)
def client(monkeypatch, database):
    monkeypatch.setenv("DB_HOST", database.host)
    monkeypatch.setenv("DB_PORT", str(database.port))
    monkeypatch.setenv("DB_NAME", database.dbname)
    monkeypatch.setenv("DB_USER", database.user)
    monkeypatch.setenv("DB_PASSWORD", database.password)
    monkeypatch.setenv("DB_TABLE_NAME", "space2stats")

    from space2stats.app import app

    with TestClient(app) as app:
        yield app


def test_read_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Space2Stats!"}


@patch("space2stats.main._get_summaries")
def test_get_summary(mock_get_summaries, client):
    mock_get_summaries.return_value = (
        [("hex_1", 100, 200)],
        ["hex_id", "sum_pop_2020", "sum_pop_f_10_2020"]
    )

    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
    }

    response = client.post("/summary", json=request_payload)

    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        for field in request_payload["fields"]:
            assert field in summary
        # +1 for the 'hex_id'
        assert len(summary) == len(request_payload["fields"]) + 1


@patch("space2stats.main._get_summaries")
def test_get_summary_with_geometry_polygon(mock_get_summaries, client):
    mock_get_summaries.return_value = (
        [("hex_1", 100, 200)],
        ["hex_id", "sum_pop_2020", "sum_pop_f_10_2020"]
    )

    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "geometry": "polygon",
    }

    response = client.post("/summary", json=request_payload)

    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        assert "geometry" in summary
        assert summary["geometry"]["type"] == "Polygon"
        for field in request_payload["fields"]:
            assert field in summary
        # +1 for the 'hex_id' and +1 for 'geometry'
        assert len(summary) == len(request_payload["fields"]) + 2


@patch("space2stats.main._get_summaries")
def test_get_summary_with_geometry_point(mock_get_summaries, client):
    mock_get_summaries.return_value = (
        [("hex_1", 100, 200)],
        ["hex_id", "sum_pop_2020", "sum_pop_f_10_2020"]
    )

    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "geometry": "point",
    }

    response = client.post("/summary", json=request_payload)

    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        assert "geometry" in summary
        assert summary["geometry"]["type"] == "Point"
        for field in request_payload["fields"]:
            assert field in summary
        # +1 for the 'hex_id' and +1 for 'geometry'
        assert len(summary) == len(request_payload["fields"]) + 2


@patch("space2stats.app.get_available_fields")
def test_get_fields(mock_get_available_fields, client):
    mock_get_available_fields.return_value = ["sum_pop_2020",
                                              "sum_pop_f_10_2020",
                                              "field3"]

    response = client.get("/fields")

    assert response.status_code == 200
    response_json = response.json()

    expected_fields = ["sum_pop_2020", "sum_pop_f_10_2020", "field3"]
    for field in expected_fields:
        assert field in response_json
