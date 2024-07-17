from fastapi.testclient import TestClient
from app.main import app
import pytest
from unittest.mock import patch

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Space2Stats!"}

@patch('psycopg2.connect')
def test_get_summary(mock_connect):
    mock_cursor = mock_connect.return_value.cursor.return_value
    mock_cursor.description = [('hex_id',), ('field1',), ('field2',)]
    mock_cursor.fetchall.return_value = [('hex_1', 100, 200)]
    
    aoi = {
        "type": "Polygon",
        "coordinates": [
            [
                [-74.1, 40.6],
                [-73.9, 40.6],
                [-73.9, 40.8],
                [-74.1, 40.8],
                [-74.1, 40.6]
            ]
        ]
    }
    
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["field1", "field2"]
    }

    response = client.post("/summary", json=request_payload)
    
    assert response.status_code == 200
    response_json = response.json()
    print(response_json)  
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        assert "fields" in summary
        for field in request_payload["fields"]:
            assert field in summary["fields"]
        assert len(summary["fields"]) == len(request_payload["fields"])

@patch('psycopg2.connect')
def test_get_fields(mock_connect):
    mock_cursor = mock_connect.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = [("hex_id",), ("field1",), ("field2",), ("field3",)]

    response = client.get("/fields")
    
    assert response.status_code == 200
    assert response.json() == ["field1", "field2", "field3"]

if __name__ == "__main__":
    pytest.main()