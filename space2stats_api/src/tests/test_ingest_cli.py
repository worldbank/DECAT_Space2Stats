import os

from space2stats_ingest.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_download_command(tmpdir, s3_mock):
    s3_path = "s3://mybucket/myfile.parquet"
    parquet_file = tmpdir.join("local.parquet")

    s3_mock.put_object(
        Bucket="mybucket", Key="myfile.parquet", Body=b"mock_parquet_data"
    )

    result = runner.invoke(
        app, ["download", s3_path, "--local-path", str(parquet_file)]
    )
    print(result.output)

    assert result.exit_code == 0
    assert "Starting download from S3" in result.stdout
    assert "Download complete" in result.stdout
    assert os.path.exists(parquet_file)


def test_load_command(tmpdir, database):
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"
    parquet_file = tmpdir.join("local.parquet")

    with open(parquet_file, "wb") as f:
        f.write(b"mock_parquet_data")

    result = runner.invoke(
        app, ["load", connection_string, "--parquet-file", str(parquet_file)]
    )
    print(result.output)

    assert result.exit_code == 0
    assert "Loading data into PostgreSQL" in result.stdout


def test_download_and_load_command(tmpdir, database, s3_mock):
    s3_path = "s3://mybucket/myfile.parquet"
    parquet_file = tmpdir.join("local.parquet")
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"

    s3_mock.put_object(
        Bucket="mybucket", Key="myfile.parquet", Body=b"mock_parquet_data"
    )

    result = runner.invoke(
        app,
        [
            "download-and-load",
            s3_path,
            connection_string,
            "--parquet-file",
            str(parquet_file),
        ],
    )
    print(result.output)

    assert result.exit_code == 0
    assert "Starting download from S3" in result.stdout
    assert "Loading data into PostgreSQL" in result.stdout
