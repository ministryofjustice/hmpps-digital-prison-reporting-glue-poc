import datetime

from pyspark import Row
from pyspark.sql.functions import col


from src.gg_logs_to_parquet import add_hash_drop_tokens, union_dfs, add_partitions_from_op_ts, PARTITION_BY


def test_add_hash_drop_tokens(spark_session):
    read_path = "./tests/data/dummy/kinesis/transac/json/oms_owner/offenders"

    local_df_i = spark_session.read.json(read_path + "/inserts/")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = spark_session.read.json(read_path + "/updates/")
    local_df_u = add_hash_drop_tokens(
        frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = spark_session.read.json(read_path + "/deletes/")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    assert local_df_i.select("after_hash").filter(col("after.offender_id").isin({150})).collect()[0] == Row(
        after_hash=-1007943897
    )
    assert local_df_u.select("before_hash").filter(col("before.offender_id").isin({150})).collect()[0] == Row(
        before_hash=-1007943897
    )
    assert local_df_u.select("after_hash").filter(col("before.offender_id").isin({150})).collect()[0] == Row(
        after_hash=251176445
    )
    assert local_df_d.select("before_hash").filter(col("before.offender_id").isin({140})).collect()[0] == Row(
        before_hash=-852450643
    )


def test_union_dfs(spark_session):
    read_path = "./tests/data/dummy/kinesis/transac/json/oms_owner/offenders"

    local_df_i = spark_session.read.json(read_path + "/inserts/")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = spark_session.read.json(read_path + "/updates/")
    local_df_u = add_hash_drop_tokens(
        frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = spark_session.read.json(read_path + "/deletes/")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    local_df_out = union_dfs(prime_df=local_df_i, df_list=[
                             local_df_u, local_df_d])

    assert local_df_out.count() == local_df_i.count() + \
        local_df_u.count() + local_df_d.count()

    assert local_df_out.count() == 3897


def test_add_partitions_from_op_ts(spark_session):
    read_path = "./tests/data/dummy/kinesis/transac/json/oms_owner/offenders"
    write_path = "./tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders"

    local_df_i = spark_session.read.json(read_path + "/inserts/")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = spark_session.read.json(read_path + "/updates/")
    local_df_u = add_hash_drop_tokens(
        frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = spark_session.read.json(read_path + "/deletes/")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    local_df_out = union_dfs(prime_df=local_df_i, df_list=[
                             local_df_u, local_df_d])

    local_df_out = add_partitions_from_op_ts(
        partition_by=PARTITION_BY, frame=local_df_out)

    assert local_df_out.select("part_date").filter(
        col("after.offender_id").isin({150}) & col("op_type").isin({"I"})
    ).collect()[0] == Row(date=datetime.date(2022, 9, 7))

    assert local_df_out.select("part_date").filter(
        col("before.offender_id").isin({144}) & col("op_type").isin({"U"})
    ).collect()[0] == Row(date=datetime.date(2022, 9, 20))

    assert local_df_out.select("part_date").filter(
        col("before.offender_id").isin({139}) & col("op_type").isin({"D"})
    ).collect()[0] == Row(date=datetime.date(2022, 9, 19))
    # print(write_path)
    # local_df_out.write.format("parquet").mode("overwrite").save(write_path)
