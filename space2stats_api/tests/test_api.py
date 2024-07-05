from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Space2Stats!"}

def test_get_summary():
    response = client.post("/summary", json={
        "aoi": {
            "type": "Polygon",
            "coordinates": [[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]]
        },
        "spatial_join_method": "touches",
        "fields": ["field1", "field2"]
    })
    assert response.status_code == 200
    assert "summaries" in response.json()