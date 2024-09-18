import os

import boto3
import psycopg
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from pytest_postgresql.janitor import DatabaseJanitor


@pytest.fixture
def setup_benchmark_env(monkeypatch):
    # Set environment variables required by the benchmark tests
    monkeypatch.setenv("BASE_URL", os.environ.get("BASE_URL"))
    monkeypatch.setenv("FIELDS_ENDPOINT", "/fields")
    monkeypatch.setenv("SUMMARY_ENDPOINT", "/summary")
    monkeypatch.setenv("FIELD", "sum_pop_2020")
    yield


@pytest.fixture()
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def s3_client(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def test_bucket(s3_client) -> str:
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture(scope="session")
def database(postgresql_proc):
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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS space2stats (
                        hex_id TEXT PRIMARY KEY,
                        sum_pop_2020 INT,
                        sum_pop_f_10_2020 INT
                    );
                """)
                cur.execute("""
                    INSERT INTO space2stats (hex_id, sum_pop_2020, sum_pop_f_10_2020)
                    VALUES ('hex_1', 100, 200), ('hex_2', 150, 250);
                """)

        yield jan


@pytest.fixture(autouse=True)
def client(monkeypatch, database, test_bucket):
    monkeypatch.setenv("PGHOST", database.host)
    monkeypatch.setenv("PGPORT", str(database.port))
    monkeypatch.setenv("PGDATABASE", database.dbname)
    monkeypatch.setenv("PGUSER", database.user)
    monkeypatch.setenv("PGPASSWORD", database.password)
    monkeypatch.setenv("PGTABLENAME", "space2stats")
    monkeypatch.setenv("S3_BUCKET_NAME", test_bucket)

    from space2stats.api import app

    with TestClient(app) as test_client:
        yield test_client
