import json

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from space2stats_ingest.main import load_parquet_to_db


def test_load_parquet_to_db(clean_database, tmpdir):
    connection_string = f"postgresql://{clean_database.user}:{clean_database.password}@{clean_database.host}:{clean_database.port}/{clean_database.dbname}"

    parquet_file = tmpdir.join("local.parquet")
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

    load_parquet_to_db(str(parquet_file), connection_string, str(item_file))

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 100, 150)

            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_2'")
            result = cur.fetchone()
            assert result == ("hex_2", 200, 250)


def test_updating_table(clean_database, tmpdir):
    connection_string = f"postgresql://{clean_database.user}:{clean_database.password}@{clean_database.host}:{clean_database.port}/{clean_database.dbname}"

    parquet_file = tmpdir.join("local.parquet")
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

    load_parquet_to_db(str(parquet_file), connection_string, str(item_file))

    update_item_file = tmpdir.join("space2stats_population_2020.json")
    update_parquet_file = tmpdir.join("update_local_parquet.json")
    update_data = {
        "hex_id": ["hex_1", "hex_2"],
        "nighttime_lights": [10_000, 20_000],
    }
    update_table = pa.table(update_data)
    pq.write_table(update_table, update_parquet_file)

    update_stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_nighttime_lights_2020",
        "properties": {
            "table:columns": [
                {"name": "hex_id", "type": "string"},
                {"name": "nighttime_lights", "type": "int64"},
            ],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    with open(update_item_file, "w") as f:
        json.dump(update_stac_item, f)

    load_parquet_to_db(
        str(update_parquet_file), connection_string, str(update_item_file)
    )

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 100, 150, 10_000)

            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_2'")
            result = cur.fetchone()
            assert result == ("hex_2", 200, 250, 20_000)


def test_columns_already_exist_in_db(clean_database, tmpdir):
    connection_string = f"postgresql://{clean_database.user}:{clean_database.password}@{clean_database.host}:{clean_database.port}/{clean_database.dbname}"

    parquet_file = tmpdir.join("local.parquet")
    data = {
        "hex_id": ["hex_1", "hex_2"],
        "existing_column": [123, 456],  # Simulates an existing column in DB
        "new_column": [789, 1011],
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
                {"name": "existing_column", "type": "int64"},
                {"name": "new_column", "type": "int64"},
            ],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    item_file = tmpdir.join("space2stats_population_2020.json")
    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    load_parquet_to_db(str(parquet_file), connection_string, str(item_file))

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 123, 789)  # Verify no duplicates


def test_rollback_on_update_failure(clean_database, tmpdir):
    connection_string = f"postgresql://{clean_database.user}:{clean_database.password}@{clean_database.host}:{clean_database.port}/{clean_database.dbname}"

    parquet_file = tmpdir.join("local.parquet")
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

    item_file = tmpdir.join("space2stats_population_2020.json")
    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    load_parquet_to_db(str(parquet_file), connection_string, str(item_file))

    # Invalid Parquet without `hex_id`
    update_parquet_file = tmpdir.join("update_local.parquet")
    update_data = {
        "new_column": [1000, 2000],
    }
    update_table = pa.table(update_data)
    pq.write_table(update_table, update_parquet_file)

    update_item_file = tmpdir.join("update_item.json")
    update_stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_population_2021",
        "properties": {
            "table:columns": [{"name": "new_column", "type": "int64"}],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    with open(update_item_file, "w") as f:
        json.dump(update_stac_item, f)

    try:
        load_parquet_to_db(
            str(update_parquet_file), connection_string, str(update_item_file)
        )
    except ValueError:
        pass

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'space2stats'"
            )
            columns = [row[0] for row in cur.fetchall()]
            assert "new_column" not in columns  # Verify no unwanted columns were added


def test_hex_id_column_mandatory(clean_database, tmpdir):
    connection_string = f"postgresql://{clean_database.user}:{clean_database.password}@{clean_database.host}:{clean_database.port}/{clean_database.dbname}"

    parquet_file = tmpdir.join("missing_hex_id.parquet")
    data = {
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

    item_file = tmpdir.join("space2stats_population_2020.json")
    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    try:
        load_parquet_to_db(str(parquet_file), connection_string, str(item_file))
    except ValueError as e:
        assert "The 'hex_id' column is missing from the Parquet file." in str(e)


def test_case_sensitivity_in_columns(clean_database, tmpdir):
    connection_string = f"postgresql://{clean_database.user}:{clean_database.password}@{clean_database.host}:{clean_database.port}/{clean_database.dbname}"

    # Create Parquet file with a column name that includes capitalization
    parquet_file = tmpdir.join("case_sensitivity.parquet")
    data = {
        "Hex_ID": ["hex_1", "hex_2"],  # Capitalized column name
        "Sum_Pop": [100, 200],
    }
    table = pa.table(data)
    pq.write_table(table, parquet_file)

    # Create corresponding STAC item with matching capitalization
    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "space2stats_case_sensitivity",
        "properties": {
            "table:columns": [
                {"name": "Hex_ID", "type": "string"},
                {"name": "Sum_Pop", "type": "int64"},
            ],
            "datetime": "2024-10-07T11:21:25.944150Z",
        },
        "geometry": None,
        "bbox": [-180, -90, 180, 90],
        "links": [],
        "assets": {},
    }

    item_file = tmpdir.join("case_sensitivity.json")
    with open(item_file, "w") as f:
        json.dump(stac_item, f)

    # Attempt to load into the database
    load_parquet_to_db(str(parquet_file), connection_string, str(item_file))

    # Validate the data was inserted correctly
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_1'")
            result = cur.fetchone()
            assert result == ("hex_1", 100)

            cur.execute("SELECT * FROM space2stats WHERE hex_id = 'hex_2'")
            result = cur.fetchone()
            assert result == ("hex_2", 200)
