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
def clean_database(postgresql_proc):
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname="cleantestdb",
        version=postgresql_proc.version,
        password="password",
    ) as jan:
        yield jan


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
    monkeypatch.setenv("TIMESERIES_TABLE_NAME", "climate")


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
def pop_stac_item_path():
    return "./space2stats_ingest/METADATA/stac/space2stats-collection/space2stats_population_2020/space2stats_population_2020.json"


@pytest.fixture
def metadata_excel_file_path():
    return "./space2stats_ingest/METADATA/Space2Stats Metadata Content.xlsx"


@pytest.fixture(scope="function")
def setup_timeseries_data(database):
    """Set up timeseries test data in the database."""
    db_url = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Create timeseries table
            cur.execute(
                """
                CREATE TABLE climate (
                    hex_id TEXT,
                    date DATE,
                    field1 FLOAT,
                    field2 FLOAT
                );
                """
            )
            conn.commit()

            # Insert sample timeseries data
            cur.execute(
                """
                INSERT INTO climate (hex_id, date, field1, field2)
                VALUES 
                    ('8611822e7ffffff', '2023-01-01', 10, 20),
                    ('8611822e7ffffff', '2023-01-02', 15, 25),
                    ('8611822e7ffffff', '2023-01-03', 20, 30),
                    ('8611823e3ffffff', '2023-01-01', 5, 15),
                    ('8611823e3ffffff', '2023-01-02', 10, 20),
                    ('8611823e3ffffff', '2023-01-03', 15, 25);
                """
            )
            conn.commit()

    return database


@pytest.fixture
def timeseries_data():
    """Fixture to provide sample timeseries data."""
    return [
        {
            "hex_id": "8611822e7ffffff",
            "date": "2023-01-01",
            "field1": 10,
            "field2": 20,
        },
        {
            "hex_id": "8611822e7ffffff",
            "date": "2023-01-02",
            "field1": 15,
            "field2": 25,
        },
        {
            "hex_id": "8611822e7ffffff",
            "date": "2023-01-03",
            "field1": 20,
            "field2": 30,
        },
    ]
