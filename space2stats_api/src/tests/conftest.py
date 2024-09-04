import os

import boto3
import pytest
from moto import mock_aws


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


@pytest.fixture(autouse=True)
def set_bucket_name(monkeypatch, test_bucket):
    monkeypatch.setenv("S3_BUCKET_NAME", test_bucket)
