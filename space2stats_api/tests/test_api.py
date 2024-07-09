import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Space2Stats!"}

def test_get_summary():
    aoi = {
        "type": "Polygon",
        "coordinates": [[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]]
    }

    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "within",
        "fields": ["sum_pop_f_0_2020", "sum_pop_m_0_2020"]
    }

    response = client.post("/summary", json=request_payload)
    assert response.status_code == 200

    response_json = response.json()
    assert isinstance(response_json, list)
    for summary in response_json:
        assert "hex_id" in summary
        assert "fields" in summary
        assert "sum_pop_f_0_2020" in summary["fields"]
        assert "sum_pop_m_0_2020" in summary["fields"]

if __name__ == "__main__":
    pytest.main()