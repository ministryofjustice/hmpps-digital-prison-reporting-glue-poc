from time import strptime

from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType, Row
import datetime
from src.schema.offenders import get_schema

"""
apply goldengate events log to target


"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

# use glue catalog True/False
USE_CATALOG = False

# configuration
config_gg_events = dict(
    source_bucket="dpr-demo-development-20220906101710889000000001",
    source="data/dummy/kinesis/transac/parquet",
    schema="oms_owner",
    table="offenders",
)
config_target_table = dict(
    target_bucket="dpr-demo-development-20220906101710889000000001",
    target_final="data/dummy/database",
    schema="oms_owner",
    table="offenders",
    # partition_by = ["date", "time"]
    partition_by=["create_date"],
)


def update_config():
    """
    Update configuration with elements describing paths to data
    :return: None
    """

    config_gg_events["path"] = (
        config_gg_events["source_bucket"]
        + "/"
        + config_gg_events["source"]
        + "/"
        + config_gg_events["schema"]
        + "/"
        + config_gg_events["table"]
    )

    config_target_table["path"] = (
        config_target_table["target_bucket"]
        + "/"
        + config_target_table["target_final"]
        + "/"
        + config_target_table["schema"]
        + "/"
        + config_target_table["table"]
    )


def write_catalog(gluecontext, config, frame):
    """
    write output using glue catalog
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    additionaloptions = {"enableUpdateCatalog": True, "partitionKeys": config["partition_by"]}

    gluecontext.write_dynamic_frame_from_catalog(
        frame=frame,
        database=config["schema"],
        table_name=config["table"],
        connection_type="s3",
        connection_options={"path": "s3://{}/".format(config["path"])},
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
            "path": "s3://{}/".format(config["path"]),
            "partitionKeys": config["partition_by"],
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


def read_s3_to_df(gluecontext, config, key_suffix=None, file_format="parquet"):
    """
    Read from S3 into Dataframe
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param key_suffix: suffix to be added to path
    :return: spark dataframe
    """
    read_path = "s3://{}".format(config["path"])
    if key_suffix is not None:
        read_path = read_path + "/{}/".format(key_suffix)
    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"path": read_path},
        format=file_format,
    )
    # inputDF = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
    # "paths": ["s3://{}".format(write_path)]}, format="parquet")
    return input_dydf.toDF()


def create_empty_df(schema):
    return spark.createDataFrame(data=spark.sparkContext.emptyRDD(), schema=schema)


def load_sample_to_df(df):
    dfsample = df.sample(0.01)
    print(dfsample.count())
    return dfsample


def output_df():
    target_schema = get_schema()

    temp_df = create_empty_df(target_schema)
    return temp_df


def get_schema_fields_as_dict(schema):
    schema_dict = {}
    for fld in schema:
        schema_dict[fld.name] = None
    return schema_dict


def get_schema_field_type(schema, fldname):
    schema_dict = {}
    for fld in schema:
        if fld.name == fldname:
            return fld.dataType


def format_field(schema, fldname, fld_val):
    fldtype = get_schema_field_type(schema=schema, fldname=fldname)
    new_val = fld_val
    if fld_val is not None:
        if fldtype == DateType():
            # print("datetype", fldname, fld_val)
            new_val = datetime.datetime.strptime(fld_val, "%Y-%m-%d")
        if fldtype == TimestampType():
            # print("timestamptype", fldname, fld_val)
            new_val = datetime.datetime.strptime(fld_val[:26], "%Y-%m-%d %H:%M:%S.%f")
    return new_val


def mapper(row_in, schema):
    # print("row_in")
    # print(row_in["after"])
    # new_row = Row()
    # print("#####")

    new_row_dict = get_schema_fields_as_dict(schema)

    if row_in["op_type"] != "D":
        row_out = row_in["after"]
    else:
        row_out = row_in["before"]

    row_dict = row_out.asDict()

    for fld_name in row_dict:
        if fld_name.lower() == "modified_datetime":
            new_row_dict["modify_datetime"] = row_dict[fld_name]
        else:
            new_row_dict[fld_name.lower()] = format_field(
                schema=schema, fldname=fld_name.lower(), fld_val=row_dict[fld_name]
            )
    new_row_dict["admin_hash"] = row_in["after_hash"]

    new_row_dict["previous_hash"] = row_in["before_hash"]
    new_row_dict["admin_gg_pos"] = row_in["pos"]
    new_row_dict["admin_gg_op_ts"] = format_field(schema=schema, fldname="admin_gg_op_ts", fld_val=row_in["op_ts"])
    new_row_dict["admin_event_ts"] = datetime.datetime.now()
    new_row_dict["event_type"] = row_in["op_type"]
    # print(new_row_dict)
    return Row(**new_row_dict)


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    update_config()

    target_schema = get_schema(with_event_type=True)

    df_event_log = read_s3_to_df(gluecontext=glueContext, config=config_gg_events)  # , key_suffix="date=2022-09-13")
    df_event_log.select(
        # col("table"),
        # col("offender_id"),
        col("op_ts"),
        col("pos"),
        col("before_hash"),
        col("before.offender_id"),
    ).filter((col("op_type").isin({"D"}))).show(10, False)

    df_table_in = read_s3_to_df(gluecontext=glueContext, config=config_target_table)

    # df_event_log.show()
    # df_table_in.show()

    df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=target_schema)).toDF(schema=target_schema)

    new_column_name_list = list(map(lambda x: "__{}".format(x), df_event_log.columns))
    df_event_log = df_event_log.toDF(*new_column_name_list)

    df_joined = df_table_in.join(df_event_log, df_table_in["offender_id"] == df_event_log["__offender_id"], "outer")

    df_joined.select(
        # col("table"),
        col("birth_date"),
        col("admin_gg_op_ts"),
        col("admin_gg_pos"),
        col("admin_hash"),
        col("__admin_hash"),
        col("__previous_hash"),
        # to_timestamp("op_ts"),
        # datetime.datetime.strptime(col("op_ts"), "%Y-%m-%dT%H:%M:%S"),
        # col("timestamp"),
        # col("hour"),
        # col("min"),
        # col("op_type"),
        # col("pos"),
        col("offender_id"),
        col("__offender_id"),
        col("__event_type")
        # col("after.offender_id"),
        # col("before_hash"),
        # col("after_hash"),
    ).filter((col("__event_type").isin({"D"}))).show(10, False)
    # local_df_out = temp_rdd.toDF(schema=target_schema)
    # out_dyf = DynamicFrame.fromDF(local_df_out, glueContext, "out_dyf")

    # write_frame(gluecontext=glueContext, config=config_target_table, frame=out_dyf)


if __name__ == "__main__":
    start()
