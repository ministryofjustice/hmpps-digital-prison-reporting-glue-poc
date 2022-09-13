import datetime

from pyspark import Row
from pyspark.sql.functions import col

from src.merge_to_parquet import update_config, config_dict, add_hash_drop_tokens, union_dfs, add_partitions_from_op_ts


def test_update_config():
    update_config()

    test_read = config_dict["read_path"] = (
        config_dict["source_bucket"]
        + "/"
        + config_dict["target_json"]
        + "/"
        + config_dict["schema"]
        + "/"
        + config_dict["table"]
    )
    test_write = config_dict["write_path"] = (
        config_dict["target_bucket"]
        + "/"
        + config_dict["target_parquet"]
        + "/"
        + config_dict["schema"]
        + "/"
        + config_dict["table"]
    )

    assert test_write == config_dict["write_path"]
    assert test_read == config_dict["read_path"]


def test_add_hash_drop_tokens(spark_session):
    config_dict["read_path"] = (
        "./tests/" + config_dict["target_json"] + "/" + config_dict["schema"] + "/" + config_dict["table"]
    )

    local_df_i = spark_session.read.json(config_dict["read_path"] + "/inserts/")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = spark_session.read.json(config_dict["read_path"] + "/updates/")
    local_df_u = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = spark_session.read.json(config_dict["read_path"] + "/deletes/")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    assert local_df_i.select("after_hash").filter(col("after.OFFENDER_ID").isin({127})).collect()[0] == Row(
        after_hash=695065351
    )
    assert local_df_u.select("before_hash").filter(col("before.OFFENDER_ID").isin({127})).collect()[0] == Row(
        after_hash=695065351
    )
    assert local_df_u.select("after_hash").filter(col("before.OFFENDER_ID").isin({127})).collect()[0] == Row(
        after_hash=878455901
    )
    assert local_df_d.select("before_hash").filter(col("before.OFFENDER_ID").isin({127})).collect()[0] == Row(
        after_hash=695065351
    )


def test_union_dfs(spark_session):
    config_dict["read_path"] = (
        "./tests/" + config_dict["target_json"] + "/" + config_dict["schema"] + "/" + config_dict["table"]
    )

    local_df_i = spark_session.read.json(config_dict["read_path"] + "/inserts/")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = spark_session.read.json(config_dict["read_path"] + "/updates/")
    local_df_u = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = spark_session.read.json(config_dict["read_path"] + "/deletes/")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    local_df_out = union_dfs(prime_df=local_df_i, df_list=[local_df_u, local_df_d])

    assert local_df_out.count() == local_df_i.count() + local_df_u.count() + local_df_d.count()

    assert local_df_out.count() == 3931


def test_add_partitions_from_op_ts(spark_session):
    config_dict["read_path"] = (
        "./tests/" + config_dict["target_json"] + "/" + config_dict["schema"] + "/" + config_dict["table"]
    )

    local_df_i = spark_session.read.json(config_dict["read_path"] + "/inserts/")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = spark_session.read.json(config_dict["read_path"] + "/updates/")
    local_df_u = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = spark_session.read.json(config_dict["read_path"] + "/deletes/")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    local_df_out = union_dfs(prime_df=local_df_i, df_list=[local_df_u, local_df_d])

    local_df_out = add_partitions_from_op_ts(config=config_dict, frame=local_df_out)

    assert local_df_out.select("date").filter(
        col("after.OFFENDER_ID").isin({127}) & col("op_type").isin({"I"})
    ).collect()[0] == Row(date=datetime.date(2022, 9, 1))

    assert local_df_out.select("date").filter(
        col("before.OFFENDER_ID").isin({127}) & col("op_type").isin({"U"})
    ).collect()[0] == Row(date=datetime.date(2022, 9, 13))

    assert local_df_out.select("date").filter(
        col("before.OFFENDER_ID").isin({127}) & col("op_type").isin({"D"})
    ).collect()[0] == Row(date=datetime.date(2022, 9, 25))
