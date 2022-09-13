from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType

USE_CATALOG = False
config_dict = dict(
    source_bucket="dpr-demo-development-20220906101710889000000001",
    target_bucket="dpr-demo-development-20220906101710889000000001",
    source="data/dummy/source/OFFENDERS_202209061845.json",
    target_json="data/dummy/kinesis/transac/json/",
    target_parquet="data/dummy/kinesis/transac/parquet/",
    schema="oms_owner",
    table="offenders",
    # partition_by = ["date", "time"]
    partition_by=["date"],
)


def update_config():
    config_dict["read_path"] = (
        config_dict["source_bucket"]
        + "/"
        + config_dict["target_json"]
        + "/"
        + config_dict["schema"]
        + "/"
        + config_dict["table"]
    )

    config_dict["write_path"] = (
        config_dict["target_bucket"]
        + "/"
        + config_dict["target_parquet"]
        + "/"
        + config_dict["schema"]
        + "/"
        + config_dict["table"]
    )


def write_catalog(gluecontext, config, frame):
    additionaloptions = {"enableUpdateCatalog": True, "partitionKeys": config_dict["partition_by"]}

    gluecontext.write_dynamic_frame_from_catalog(
        frame=frame,
        database=config["schema"],
        table_name=config["table"],
        connection_type="s3",
        connection_options={"path": "s3://{}/".format(config["write_path"])},
        additional_options=additionaloptions,
        format="parquet",
    )


def write_s3(gluecontext, config, frame):
    gluecontext.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={
            "path": "s3://{}/".format(config["write_path"]),
            "partitionKeys": config_dict["partition_by"],
        },
        format="parquet",
    )


def write_frame(gluecontext, config, frame):
    if USE_CATALOG:
        write_catalog(gluecontext=gluecontext, config=config, frame=frame)
    else:
        write_s3(gluecontext=gluecontext, config=config, frame=frame)


def read_s3_to_df(gluecontext, config, key_suffix):
    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://{}/{}/".format(config["read_path"], key_suffix)]},
        format="json",
    )
    return input_dydf.toDF()


def add_hash_drop_tokens(frame, hash_fields):
    new_frame = frame.drop(col("tokens"))
    for hash_field in hash_fields:
        hash_name = "{}_hash".format(hash_field)
        new_frame = new_frame.withColumn(hash_name, hash(hash_field))
    return new_frame


def add_partitions_from_op_ts(config, frame):
    op_ts_def = [1, 19]
    new_frame = frame
    for part in config["partition_by"]:
        if part == "date":
            new_frame = new_frame.withColumn(
                "date", substring(col("op_ts"), op_ts_def[0], op_ts_def[1]).cast(DateType())
            )
        if part == "time":
            new_frame = new_frame.withColumn(
                "time", date_format(substring(col("op_ts"), op_ts_def[0], op_ts_def[1]).cast(TimestampType()), "HH:mm")
            )

    return new_frame


def union_dfs(prime_df, df_list):
    new_df = prime_df
    for un_df in df_list:
        new_df = new_df.unionByName(un_df, allowMissingColumns=True)
    return new_df


def start():
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    update_config()

    local_df_i = read_s3_to_df(gluecontext=glueContext, config=config_dict, key_suffix="inserts")
    local_df_i = add_hash_drop_tokens(frame=local_df_i, hash_fields=["after"])

    local_df_u = read_s3_to_df(gluecontext=glueContext, config=config_dict, key_suffix="updates")
    local_df_u = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])

    local_df_d = read_s3_to_df(gluecontext=glueContext, config=config_dict, key_suffix="deletes")
    local_df_d = add_hash_drop_tokens(frame=local_df_d, hash_fields=["before"])

    local_df_out = union_dfs(prime_df=local_df_i, df_list=[local_df_u, local_df_d])

    local_df_out = add_partitions_from_op_ts(config=config_dict, frame=local_df_out)

    local_df_out.select(
        col("table"),
        col("date"),
        col("op_type"),
        col("pos"),
        col("before.offender_id"),
        col("after.offender_id"),
        col("before_hash"),
        col("after_hash"),
    ).filter((col("before.offender_id").isin({127, 128, 129})) | (col("after.offender_id").isin({127, 128, 129}))).show(
        10
    )

    out_dyf = DynamicFrame.fromDF(local_df_out, glueContext, "out_dyf")

    write_frame(gluecontext=glueContext, config=config_dict, frame=out_dyf)


if __name__ == "__main__":
    start()
