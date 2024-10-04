import os

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from space2stats_ingest.main import download_parquet_from_s3, load_parquet_to_db


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
    stac_metadata_file = tmpdir.join("stac_metadata.json")

    data = {
        "hex_id": ["hex_1", "hex_2"],
        "sum_pop_2020": [100, 200],
        "sum_pop_f_10_2020": [300, 400],
    }
    table = pa.table(data)
    pq.write_table(table, parquet_file)

    with open(stac_metadata_file, "w") as f:
        f.write("""
        {
            "type": "Feature",
            "properties": {
                "table:columns": [
                    {"name": "hex_id", "type": "string"},
                    {"name": "sum_pop_2020", "type": "int64"},
                    {"name": "sum_pop_f_10_2020", "type": "int64"}
                ]
            }
        }
        """)

    load_parquet_to_db(str(parquet_file), connection_string, str(stac_metadata_file))

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 100, 300)

            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_2'")
            result = cur.fetchone()
            assert result == ("hex_2", 200, 400)
