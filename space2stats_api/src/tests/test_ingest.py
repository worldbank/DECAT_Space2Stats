import json
import os

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from space2stats_ingest.main import (
    download_parquet_from_s3,
    get_all_stac_fields,
    load_parquet_to_db,
)


def test_get_all_stac_fields(stac_catalog_path):
    print(stac_catalog_path)
    fields = get_all_stac_fields(stac_catalog_path, "population_2020.json")
    print(fields)
    assert (
        len(fields) > 0 and len(fields) < 100
    ), f"Fields have unexpected length: {fields}"


def test_download_parquet_from_s3(s3_mock):
    s3_path = "s3://mybucket/myfile.parquet"
    parquet_file = "local.parquet"

    s3_mock.put_object(
        Bucket="mybucket", Key="myfile.parquet", Body=b"mock_parquet_data"
    )

    download_parquet_from_s3(s3_path, parquet_file)

    assert os.path.exists(parquet_file)


def test_load_parquet_to_db(database, tmpdir):
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"

    parquet_file = tmpdir.join("local.parquet")

    catalog_file = tmpdir.join("catalog.json")
    collection_file = tmpdir.join("collection.json")
    item_file = tmpdir.join("space2stats_population_2020.json")

    data = {
        "hex_id": ["hex_1", "hex_2"],
        "sum_pop_f_10_2020": [100, 200],
        "sum_pop_m_10_2020": [150, 250],
    }

    table = pa.table(data)
    pq.write_table(table, parquet_file)

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_population_2020",
        "properties": {
            "table:columns": [
                {"name": "hex_id", "type": "string"},
                {"name": "sum_pop_f_10_2020", "type": "int64"},
                {"name": "sum_pop_m_10_2020", "type": "int64"},
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

    load_parquet_to_db(str(parquet_file), connection_string, str(catalog_file), str(item_file))

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 100, 150)

            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_2'")
            result = cur.fetchone()
            assert result == ("hex_2", 200, 250)
