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
config_dict = dict(
    source_bucket="dpr-demo-development-20220906101710889000000001",
    target_bucket="dpr-demo-development-20220906101710889000000001",
    source="data/dummy/source/OFFENDERS_202209061845.json",
    target_json="data/dummy/kinesis/transac/json/",
    target_parquet="data/dummy/kinesis/transac/parquet/",
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

    config_dict["read_path"] = (
        config_dict["source_bucket"]
        + "/"
        + config_dict["target_parquet"]
        + "/"
        + config_dict["schema"]
        + "/"
        + config_dict["table"]
    )

    config_dict["write_path"] = (
        config_dict["target_bucket"]
        + "/"
        + config_dict["target_final"]
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


def read_s3_to_df(gluecontext, config, key_suffix=None):
    """
    Read from S3 into Dataframe
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param key_suffix: suffix to be added to path
    :return: spark dataframe
    """
    read_path = "s3://{}/".format(config["read_path"])
    if key_suffix is not None:
        read_path = read_path + "{}/".format(key_suffix)
    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [read_path]},
        format="json",
    )
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
    row_out = row_in["after"]

    new_row_dict = get_schema_fields_as_dict(schema)
    row_dict = row_out.asDict()

    for fld_name in row_dict:
        if fld_name.lower() == "modified_datetime":
            new_row_dict["modify_datetime"] = row_dict[fld_name]
        else:
            new_row_dict[fld_name.lower()] = format_field(
                schema=schema, fldname=fld_name.lower(), fld_val=row_dict[fld_name]
            )
    new_row_dict["admin_hash"] = row_in["after_hash"]
    new_row_dict["admin_gg_pos"] = row_in["pos"]
    new_row_dict["admin_gg_op_ts"] = format_field(schema=schema, fldname="admin_gg_op_ts", fld_val=row_in["op_ts"])
    new_row_dict["admin_event_ts"] = datetime.datetime.now()
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

    target_schema = get_schema()

    df = read_s3_to_df(gluecontext=glueContext, config=config_dict, key_suffix="date=2022-09-01")
    df = load_sample_to_df()

    temp_rdd = df.rdd.map(lambda row: mapper(row_in=row, schema=target_schema))

    local_df_out = temp_rdd.toDF(schema=target_schema)
    out_dyf = DynamicFrame.fromDF(local_df_out, glueContext, "out_dyf")

    write_frame(gluecontext=glueContext, config=config_dict, frame=out_dyf)


if __name__ == "__main__":
    start()
