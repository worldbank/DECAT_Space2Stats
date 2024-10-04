import os

import pyarrow as pa
import pyarrow.parquet as pq
from space2stats_ingest.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def create_mock_parquet_file(parquet_file, columns):
    table = pa.Table.from_pydict({name: [1.0, 2.0, 3.0] for name, _ in columns})
    pq.write_table(table, parquet_file)


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
    stac_metadata_file = tmpdir.join("stac_metadata.json")

    create_mock_parquet_file(
        parquet_file, [("hex_id", pa.string()), ("mock_column", pa.float64())]
    )

    with open(stac_metadata_file, "w") as f:
        f.write("""
        {
            "type": "Feature",
            "properties": {
                "table:columns": [
                    {"name": "hex_id", "type": "string"},
                    {"name": "mock_column", "type": "float64"}
                ]
            }
        }
        """)

    result = runner.invoke(
        app,
        [
            "load",
            connection_string,
            str(stac_metadata_file),
            "--parquet-file",
            str(parquet_file),
        ],
    )
    print(result.output)

    assert result.exit_code == 0
    assert "Loading data into PostgreSQL" in result.stdout


def test_load_command_column_mismatch(tmpdir, database):
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"
    parquet_file = tmpdir.join("local.parquet")
    stac_metadata_file = tmpdir.join("stac_metadata.json")

    create_mock_parquet_file(parquet_file, [("different_column", pa.float64())])

    with open(stac_metadata_file, "w") as f:
        f.write("""
        {
            "type": "Feature",
            "properties": {
                "table:columns": [
                    {"name": "mock_column", "type": "float64"}
                ]
            }
        }
        """)

    result = runner.invoke(
        app,
        [
            "load",
            connection_string,
            str(stac_metadata_file),
            "--parquet-file",
            str(parquet_file),
        ],
    )
    print(result.output)

    assert result.exit_code != 0
    assert "Column mismatch" in result.stdout


def test_download_and_load_command(tmpdir, database, s3_mock):
    s3_path = "s3://mybucket/myfile.parquet"
    parquet_file = tmpdir.join("local.parquet")
    stac_metadata_file = tmpdir.join("stac_metadata.json")
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"

    create_mock_parquet_file(
        parquet_file, [("hex_id", pa.string()), ("mock_column", pa.float64())]
    )

    with open(parquet_file, "rb") as f:
        s3_mock.put_object(Bucket="mybucket", Key="myfile.parquet", Body=f.read())

    with open(stac_metadata_file, "w") as f:
        f.write("""
        {
            "type": "Feature",
            "properties": {
                "table:columns": [
                    {"name": "hex_id", "type": "string"},
                    {"name": "mock_column", "type": "float64"}
                ]
            }
        }
        """)

    result = runner.invoke(
        app,
        [
            "download-and-load",
            s3_path,
            connection_string,
            str(stac_metadata_file),
            "--parquet-file",
            str(parquet_file),
        ],
    )
    print(result.output)

    assert result.exit_code == 0
    assert "Starting download from S3" in result.stdout
    assert "Loading data into PostgreSQL" in result.stdout
