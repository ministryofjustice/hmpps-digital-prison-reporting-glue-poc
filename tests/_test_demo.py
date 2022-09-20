import datetime

from pyspark import Row
from pyspark.sql.functions import col

# from src._sample_target import get_schema
from src._merge_gg_logs_to_parquet_by_table import update_config, config_dict, add_hash_drop_tokens, union_dfs, add_partitions_from_op_ts


#
# def test_add_partitions_from_op_ts(spark_session):
#     config_dict["read_path"] = (
#             "./tests/" + config_dict["target_json"] + "/" + config_dict["schema"] + "/" + config_dict["table"]
#     )
#     config_dict["write_path"] = (
#             "./tests/" + config_dict["target_parquet"] + "/" + config_dict["schema"] + "/" + config_dict["table"]
#     )
#     config_dict["partition_by"] = ["part_date"]
#     local_df_i = spark_session.read.json(config_dict["read_path"] + "/inserts/")
#     local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])
#
#     local_df_u = spark_session.read.json(config_dict["read_path"] + "/updates/")
#     local_df_u = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])
#
#     local_df_d = spark_session.read.json(config_dict["read_path"] + "/deletes/")
#     local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])
#
#     local_df_out = union_dfs(prime_df=local_df_i, df_list=[local_df_u, local_df_d])
#
#     local_df_out = add_partitions_from_op_ts(config=config_dict, frame=local_df_out)
#
#     local_df_out.select(col("op_type"),
#                         col("before.offender_id"),
#                         col("part_date"),
#
#                         ).filter(col("op_type").isin({"D"})).show()


# assert local_df_out.select("part_date").filter(
#     col("after.offender_id").isin({150}) & col("op_type").isin({"I"})
# ).collect()[0] == Row(date=datetime.date(2022, 9, 7))
#
# assert local_df_out.select("part_date").filter(
#     col("before.offender_id").isin({144}) & col("op_type").isin({"U"})
# ).collect()[0] == Row(date=datetime.date(2022, 9, 16))
#
# assert local_df_out.select("part_date").filter(
#     col("before.offender_id").isin({140}) & col("op_type").isin({"D"})
# ).collect()[0] == Row(date=datetime.date(2022, 9, 17))
# print(config_dict["target_parquet"])
# local_df_out.write.format("parquet").mode("overwrite").save(config_dict["target_parquet"])


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

    local_df_d.select(col("op_type"),
                      col("before.offender_id"),
                      col("before_hash"),


                      ).show()
    # .filter(col("before.offender_id").isin({1061, 873, 150, 127, 128, 129}))

# assert local_df_i.select("after_hash").filter(col("after.offender_id").isin({68})).collect()[0] == Row(
#     after_hash=110364953
# )
# assert local_df_u.select("before_hash").filter(col("before.offender_id").isin({68})).collect()[0] == Row(
#     before_hash=-1818312449
# )
# assert local_df_u.select("after_hash").filter(col("before.offender_id").isin({68})).collect()[0] == Row(
#     after_hash=-378126805
# )
# assert local_df_d.select("before_hash").filter(col("before.offender_id").isin({140})).collect()[0] == Row(
#     before_hash=-530534912
# )
