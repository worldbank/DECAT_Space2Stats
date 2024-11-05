import os

import boto3
import psycopg
import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature
from moto import mock_aws
from pytest_postgresql.janitor import DatabaseJanitor
from space2stats.api.app import build_app


@pytest.fixture
def s3_mock():
    """Mock S3 environment and create a test bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")
        yield s3


@pytest.fixture()
def aws_credentials():
    """Mocked AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def database(postgresql_proc):
    """Set up a PostgreSQL database for testing and clean up afterwards."""
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname="testdb",
        version=postgresql_proc.version,
        password="password",
    ) as jan:
        db_url = (
            f"postgresql://{jan.user}:{jan.password}@{jan.host}:{jan.port}/{jan.dbname}"
        )
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE space2stats (
                        hex_id TEXT PRIMARY KEY,
                        sum_pop_2020 INT,
                        sum_pop_f_10_2020 INT
                    );
                    """
                )
                conn.commit()

                # Insert data that corresponds to the expected H3 IDs
                cur.execute(
                    """
                    INSERT INTO space2stats (hex_id, sum_pop_2020, sum_pop_f_10_2020)
                    VALUES 
                        ('862a1070fffffff', 100, 200), 
                        ('862a10767ffffff', 150, 250), 
                        ('862a1073fffffff', 120, 220),
                        ('867a74817ffffff', 125, 225),
                        ('867a74807ffffff', 125, 225); 
                    """
                )
                conn.commit()

        yield jan


@pytest.fixture(autouse=True)
def mock_env(monkeypatch, database):
    """Automatically set environment variables for PostgreSQL and S3."""
    monkeypatch.setenv("PGHOST", database.host)
    monkeypatch.setenv("PGPORT", str(database.port))
    monkeypatch.setenv("PGDATABASE", database.dbname)
    monkeypatch.setenv("PGUSER", database.user)
    monkeypatch.setenv("PGPASSWORD", database.password)
    monkeypatch.setenv("PGTABLENAME", "space2stats")
    monkeypatch.setenv("S3_BUCKET_NAME", "mybucket")


@pytest.fixture
def client():
    """Provide a test client for FastAPI."""
    app = build_app()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def aoi_example():
    """Provide an example AOI feature for testing."""
    return Feature(
        type="Feature",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [
                    [41.14127371265408, -2.1034653113510444],
                    [41.140645873470845, -2.104696345752785],
                    [41.14205369446421, -2.104701102391104],
                    [41.14127371265408, -2.1034653113510444],
                ]
            ],
        },
        properties={},
    )


@pytest.fixture
def stac_catalog_path():
    return "./space2stats_ingest/METADATA/stac/catalog.json"


@pytest.fixture
def stac_item_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(current_dir, "../../.."))
    json_file_path = os.path.join(
        root_dir,
        "space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/space2stats_population_2020/space2stats_population_2020.json",
    )
    return json_file_path
