from src.apply_to_curated import apply_changes


def test_apply_to_curated(spark_session):
    read_table = "tests/data/dummy/database/oms_owner/offenders/"

    df_table_in = spark_session.read.parquet(read_table)

    df_table_out = apply_changes(table_in_df=df_table_in)

    assert df_table_in == df_table_out
