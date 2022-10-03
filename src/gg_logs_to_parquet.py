from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType
import boto3

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

DATABASE_NAME = "dpr-glue-catalog-development-development"
GG_TRANSAC_EVENTS_TABLE_IN = "goldengate_transac_raw_json_logs"
GG_TRANSAC_EVENTS_TABLE_OUT = "goldengate_transac_parquet_logs"
PARTITION_BY = ["part_date"]


# configuration

# use glue catalog True/False
USE_CATALOG = False


def get_table_location(database, table_name):
    """
    get table data location from glue catalogue
    :param database: database name
    :param table_name: table name
    :return: full path to data
    """
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    return table_def["Table"]["StorageDescriptor"]["Location"]


def read_catalog(gluecontext, database, tablename):
    """
    Read data via catalogue in dataframe containing data from table as defined in glue catalogue
    :param gluecontext: context
    :param database: database name
    :param tablename: table name
    :return: dataframe
    """
    input_dydf = gluecontext.create_data_frame.from_catalog(
        database=database, table_name=tablename, transformation_ctx="df"
    )
    return input_dydf.toDF()


def read_table(gluecontext, database, tablename, format="parquet"):
    """
    Read from S3 into Dataframe
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param key_suffix: suffix to be added to path
    :return: spark dataframe
    """
    read_path = get_table_location(database, tablename)

    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [read_path]},
        format=format,
    )
    return input_dydf.toDF()


def read_frame(gluecontext, database, tablename, format="parquet"):
    """
    wrapper for write mechanism determined by USE_CATALOG
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    if USE_CATALOG:
        ret_df = read_catalog(gluecontext=gluecontext, database=database, tablename=tablename, format=format)
    else:
        ret_df = read_table(gluecontext=gluecontext, database=database, tablename=tablename, format=format)

    return ret_df


def write_catalog(gluecontext, database, tablename, frame, format="parquet"):
    """
    write output using glue catalog
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    additionaloptions = {"enableUpdateCatalog": True, "partitionKeys": PARTITION_BY}

    gluecontext.write_dynamic_frame_from_catalog(
        frame=frame,
        database=database,
        table_name=tablename,
        additional_options=additionaloptions,
        format=format,
    )


def write_table(gluecontext, database, tablename, frame, format="parquet"):
    """
    write output to S3
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    write_path = get_table_location(database, tablename)
    gluecontext.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={
            "path": write_path,
            "partitionKeys": PARTITION_BY,
        },
        format=format,
    )


def write_frame(gluecontext, database, tablename, frame, format="parquet"):
    """
    wrapper for write mechanism determined by USE_CATALOG
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    if USE_CATALOG:
        write_catalog(gluecontext=gluecontext, database=database, tablename=tablename, frame=frame, format=format)
    else:
        write_table(gluecontext=gluecontext, database=database, tablename=tablename, frame=frame, format=format)


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


def add_partitions_from_op_ts(partition_by, frame):
    """
    Add partition fields to dataframe
    :param config: configuration dictionary
    :param frame: spark dataframe
    :return: spark dataframe
    """
    op_ts_def = [1, 19]
    new_frame = frame
    for part in partition_by:
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
        col(PARTITION_BY[0]),
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

    """        
    1. Read in event log as json
    2. add hash field for record contents (before and or after where relevant) and drop tokens

    """

    local_df_u = read_frame(
        gluecontext=glueContext, database=DATABASE_NAME, tablename=GG_TRANSAC_EVENTS_TABLE_IN, format="json"
    )

    local_df_out = add_hash_drop_tokens(frame=local_df_u, hash_fields=["after", "before"])

    """3. Union together"""

    """4. Add Partition field(s)"""

    local_df_out = add_partitions_from_op_ts(partition_by=PARTITION_BY, frame=local_df_out)

    show_frame(local_df_out)

    """5. write to target"""
    out_dyf = DynamicFrame.fromDF(local_df_out, glueContext, "out_dyf")

    write_frame(gluecontext=glueContext, database=DATABASE_NAME, tablename=GG_TRANSAC_EVENTS_TABLE_OUT, frame=out_dyf)


if __name__ == "__main__":
    start()
