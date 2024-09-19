from space2stats import StatsTable, Settings


def test_stats_table(mock_env):
    with StatsTable.connect() as stats_table:
        assert stats_table.table_name == "space2stats"
        assert stats_table.conn.closed == 0
        stats_table.conn.execute("SELECT 1")


def test_stats_table_connect(mock_env, database):
    with StatsTable.connect(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="XYZ",
    ) as stats_table:
        assert stats_table.table_name == "XYZ"
        assert stats_table.conn.closed == 0
        stats_table.conn.execute("SELECT 1")


def test_stats_table_settings(mock_env, database):
    settings = Settings(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="ABC",
    )
    with StatsTable.connect(settings) as stats_table:
        assert stats_table.table_name == "ABC"
        assert stats_table.conn.closed == 0
        stats_table.conn.execute("SELECT 1")
