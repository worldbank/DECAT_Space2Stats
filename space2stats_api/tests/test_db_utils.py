import unittest
from unittest.mock import patch, Mock
from app.utils.db_utils import get_summaries, get_available_fields
from psycopg.sql import SQL, Identifier

@patch("psycopg.connect")
def test_get_summaries(mock_connect):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.description = [("hex_id",), ("field1",), ("field2",)]
    mock_cursor.fetchall.return_value = [("hex_1", 100, 200)]

    fields = ["field1", "field2"]
    h3_ids = ["hex_1"]
    rows, colnames = get_summaries(fields, h3_ids)

    mock_connect.assert_called_once()
    sql_query = SQL(
        """
            SELECT {0}
            FROM {1}
            WHERE hex_id = ANY (%s)
        """
    ).format(
        SQL(', ').join([Identifier(c) for c in ['hex_id'] + fields]),
        Identifier("space2stats")
    )
    mock_cursor.execute.assert_called_once_with(sql_query, [h3_ids])

    assert rows == [("hex_1", 100, 200)]
    assert colnames == ["hex_id", "field1", "field2"]

@patch("psycopg.connect")
def test_get_available_fields(mock_connect):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchall.return_value = [("field1",), ("field2",), ("field3",)]

    columns = get_available_fields()

    mock_connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with(
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = %s
    """,
        ["space2stats"]
    )

    assert columns == ["field1", "field2", "field3"]

if __name__ == "__main__":
    unittest.main()