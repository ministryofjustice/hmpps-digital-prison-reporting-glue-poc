from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType

"""
merge goldengate events log to parquet 
    Resolution:
        Ingest raw goldengate events logs 
        commit goldengate events log to parquet on s3

    Logical steps:
        1. Read in event log as json
        2. add hash field for record contents (before and or after where relevant) and drop tokens
        3. Union together
        4. Add Partition field(s)
        5. write to target

   Notes : To run in glue_etl docker
        1. Copy this script to /jupyter_workspace/src
        3. execute with delta support (see readme) 

    ToDo: 
        refactor methods into src/lib/ - maybe not possible in glue etl
        enhance commentary
        resolve dynamic frame read/write catalog (requires glue catalog)



"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

BUCKET_SUFFIX = "20220916083016121000000001"
# use glue catalog True/False
USE_CATALOG = False

# configuration
config_dict = dict(
    source_bucket="dpr-demo-development-{}".format(BUCKET_SUFFIX),
    target_bucket="dpr-demo-development-{}".format(BUCKET_SUFFIX),
    target_json="data/dummy/kinesis/transac/json/",
    target_parquet="data/dummy/kinesis/transac/parquet/",
    schema="oms_owner",
    table="all",
    partition_by=["part_date"],
)


def update_config():
    """
    Update configuration with elements describing paths to data
    :return: None
    """

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
    """
    write output using glue catalog
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
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
    """
    write output to S3
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
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
    """
    wrapper for write mechanism determined by USE_CATALOG
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    if USE_CATALOG:
        write_catalog(gluecontext=gluecontext, config=config, frame=frame)
    else:
        write_s3(gluecontext=gluecontext, config=config, frame=frame)


def read_s3_to_df(gluecontext, config, ):
    """
    Read from S3 into Dataframe
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param key_suffix: suffix to be added to path
    :return: spark dataframe
    """
    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://{}/".format(config["read_path"])]},
        format="json",
    )
    return input_dydf.toDF()


def add_hash_drop_tokens(frame, hash_fields):
    """
    drop tokens fields from goldengate data and add hash of records event pertains to
    :param frame: spark dataframe
    :param hash_fields: fields to be hashed (before and/or after)
    :return: spark dataframe
    """
    new_frame = frame.drop(col("tokens"))
    for hash_field in hash_fields:
        hash_name = "{}_hash".format(hash_field)
        new_frame = new_frame.withColumn(hash_name, hash(hash_field))
    return new_frame


def add_partitions_from_op_ts(config, frame):
    """
    Add partition fields to dataframe
    :param config: configuration dictionary
    :param frame: spark dataframe
    :return: spark dataframe
    """
    op_ts_def = [1, 19]
    new_frame = frame
    for part in config["partition_by"]:
        if part == "part_date":
            new_frame = new_frame.withColumn(
                "part_date", substring(col("op_ts"), op_ts_def[0], op_ts_def[1]).cast(DateType())
            )
        if part == "part_time":
            new_frame = new_frame.withColumn(
                "part_time",
                date_format(substring(col("op_ts"), op_ts_def[0], op_ts_def[1]).cast(TimestampType()), "HH:mm"),
            )

    return new_frame


def show_frame(frame):
    frame.select(
        col("table"),
        col(config_dict["partition_by"][0]),
        col("op_type"),
        col("pos"),
        col("before.offender_id"),
        col("after.offender_id"),
        col("before_hash"),
        col("after_hash"),
    ).filter((col("before.offender_id").isin({127, 128, 129})) | (col("after.offender_id").isin({127, 128, 129}))).show(
        10
    )


def union_dfs(prime_df, df_list):
    """
    union dataframes into one
    :param prime_df: primary dataframe
    :param df_list: list of frames to be unioned to primary
    :return: spark dataframe
    """
    new_df = prime_df
    for un_df in df_list:
        new_df = new_df.unionByName(un_df, allowMissingColumns=True)
    return new_df


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    update_config()
    """        
    1. Read in event log as json
    2. add hash field for record contents (before and or after where relevant) and drop tokens

    """

    local_df_u = read_s3_to_df(gluecontext=glueContext, config=config_dict, )
    local_df_out = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])

    """3. Union together"""

    """4. Add Partition field(s)"""

    local_df_out = add_partitions_from_op_ts(config=config_dict, frame=local_df_out)

    show_frame(local_df_out)

    """5. write to target"""
    out_dyf = DynamicFrame.fromDF(local_df_out, glueContext, "out_dyf")

    write_frame(gluecontext=glueContext, config=config_dict, frame=out_dyf)


if __name__ == "__main__":
    start()
