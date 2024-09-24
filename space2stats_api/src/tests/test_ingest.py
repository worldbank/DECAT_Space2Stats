import os

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from space2stats_ingest.main import download_parquet_from_s3, load_parquet_to_db


def test_download_parquet_from_s3(s3_mock):
    s3_path = "s3://mybucket/myfile.parquet"
    local_path = "local.parquet"

    s3_mock.put_object(
        Bucket="mybucket", Key="myfile.parquet", Body=b"mock_parquet_data"
    )

    download_parquet_from_s3(s3_path, local_path)

    assert os.path.exists(local_path)


def test_load_parquet_to_db(database):
    connection_string = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"
    parquet_file = "local.parquet"

    data = {
        "hex_id": ["hex_1", "hex_2"],
        "sum_pop_2020": [100, 200],
        "sum_pop_f_10_2020": [300, 400],
    }
    table = pa.table(data)
    pq.write_table(table, parquet_file)

    load_parquet_to_db(parquet_file, connection_string)

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 100, 300)

            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_2'")
            result = cur.fetchone()
            assert result == ("hex_2", 200, 400)
