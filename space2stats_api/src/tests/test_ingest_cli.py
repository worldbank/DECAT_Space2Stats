import json
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
    catalog_file = tmpdir.join("catalog.json")
    collection_file = tmpdir.join("collection.json")
    item_file = tmpdir.join("space2stats_population_2020.json")

    create_mock_parquet_file(
        parquet_file, [("hex_id", pa.string()), ("mock_column", pa.float64())]
    )

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_population_2020",
        "properties": {
            "table:columns": [
                {"name": "hex_id", "type": "string"},
                {"name": "mock_column", "type": "int64"},
            ],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "space2stats-collection",
        "description": "Test collection for Space2Stats.",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
        "links": [{"rel": "item", "href": str(item_file), "type": "application/json"}],
    }

    with open(collection_file, "w") as f:
        json.dump(stac_collection, f)

    stac_catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "space2stats-catalog",
        "description": "Test catalog for Space2Stats.",
        "license": "CC-BY-4.0",
        "links": [
            {"rel": "child", "href": str(collection_file), "type": "application/json"}
        ],
    }

    with open(catalog_file, "w") as f:
        json.dump(stac_catalog, f)

    result = runner.invoke(
        app,
        [
            "load",
            connection_string,
            str(catalog_file),
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
    catalog_file = tmpdir.join("catalog.json")
    collection_file = tmpdir.join("collection.json")
    item_file = tmpdir.join("space2stats_population_2020.json")

    create_mock_parquet_file(parquet_file, [("different_column", pa.float64())])

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_population_2020",
        "properties": {
            "table:columns": [{"name": "mock_column", "type": "float64"}],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "space2stats-collection",
        "description": "Test collection for Space2Stats.",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
        "links": [{"rel": "item", "href": str(item_file), "type": "application/json"}],
    }

    with open(collection_file, "w") as f:
        json.dump(stac_collection, f)

    stac_catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "space2stats-catalog",
        "description": "Test catalog for Space2Stats.",
        "license": "CC-BY-4.0",
        "links": [
            {"rel": "child", "href": str(collection_file), "type": "application/json"}
        ],
    }

    with open(catalog_file, "w") as f:
        json.dump(stac_catalog, f)

    result = runner.invoke(
        app,
        [
            "load",
            connection_string,
            str(catalog_file),
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
    catalog_file = tmpdir.join("catalog.json")
    collection_file = tmpdir.join("collection.json")
    item_file = tmpdir.join("space2stats_population_2020.json")
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"

    create_mock_parquet_file(
        parquet_file, [("hex_id", pa.string()), ("mock_column", pa.float64())]
    )

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_population_2020",
        "properties": {
            "table:columns": [
                {"name": "hex_id", "type": "string"},
                {"name": "mock_column", "type": "float64"},
            ],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "space2stats-collection",
        "description": "Test collection for Space2Stats.",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
        "links": [{"rel": "item", "href": str(item_file), "type": "application/json"}],
    }

    with open(collection_file, "w") as f:
        json.dump(stac_collection, f)

    stac_catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "space2stats-catalog",
        "description": "Test catalog for Space2Stats.",
        "license": "CC-BY-4.0",
        "links": [
            {"rel": "child", "href": str(collection_file), "type": "application/json"}
        ],
    }

    with open(catalog_file, "w") as f:
        json.dump(stac_catalog, f)

    with open(parquet_file, "rb") as f:
        s3_mock.put_object(Bucket="mybucket", Key="myfile.parquet", Body=f.read())

    result = runner.invoke(
        app,
        [
            "download-and-load",
            s3_path,
            connection_string,
            str(catalog_file),
            "--parquet-file",
            str(parquet_file),
        ],
    )
    print(result.output)

    assert result.exit_code == 0
    assert "Starting download from S3" in result.stdout
    assert "Loading data into PostgreSQL" in result.stdout
