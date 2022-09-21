from time import strptime

# from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType, Row
import datetime

"""
create a target table from dummy gg events at base


"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

# use glue catalog True/False
USE_CATALOG = False

from pyspark.sql.types import (
    StringType,
    StructType,
    LongType,
    DoubleType,
    FloatType,
    IntegerType,
    BooleanType,
    BinaryType,
    ArrayType,
    MapType,
    DateType,
    TimestampType,
    StructField,
)

possible_types = {
    1: lambda t: DoubleType(),
    2: lambda t: FloatType(),
    3: lambda t: LongType(),
    4: lambda t: LongType(),
    5: lambda t: IntegerType(),
    8: lambda t: BooleanType(),
    "varchar2": lambda t: StringType(),
    # 11: lambda t: schema_for_spark(t.message_type),
    12: lambda t: BinaryType(),
    "number": lambda t: IntegerType(),
    14: lambda t: StringType(),  # enum type
    15: lambda t: IntegerType(),
    16: lambda t: LongType(),
    17: lambda t: IntegerType(),
    18: lambda t: LongType(),
    "date": lambda t: DateType(),
    "timestamp": lambda t: TimestampType(),
}


def __type_for(datatype):
    """
    return types for field
    """
    get_type = possible_types.get(datatype, lambda t: StringType())

    return get_type(datatype)


ddl = (
    "OFFENDER_BOOK_ID NUMBER,BOOKING_BEGIN_DATE DATE,BOOKING_END_DATE DATE,BOOKING_NO VARCHAR2(56),OFFENDER_ID "
    "NUMBER,AGY_LOC_ID VARCHAR2(24),LIVING_UNIT_ID NUMBER,DISCLOSURE_FLAG VARCHAR2(4),IN_OUT_STATUS VARCHAR2(48),"
    "ACTIVE_FLAG VARCHAR2(4),BOOKING_STATUS VARCHAR2(48),YOUTH_ADULT_CODE VARCHAR2(48),FINGER_PRINTED_STAFF_ID "
    "NUMBER,SEARCH_STAFF_ID NUMBER,PHOTO_TAKING_STAFF_ID NUMBER,ASSIGNED_STAFF_ID NUMBER,CREATE_AGY_LOC_ID "
    "VARCHAR2(24),BOOKING_TYPE VARCHAR2(48),BOOKING_CREATED_DATE DATE,ROOT_OFFENDER_ID NUMBER,AGENCY_IML_ID NUMBER,"
    "SERVICE_FEE_FLAG VARCHAR2(4),EARNED_CREDIT_LEVEL VARCHAR2(48),EKSTRAND_CREDIT_LEVEL VARCHAR2(48),"
    "INTAKE_AGY_LOC_ID VARCHAR2(24),ACTIVITY_DATE DATE,INTAKE_CASELOAD_ID VARCHAR2(24),INTAKE_USER_ID VARCHAR2("
    "128),CASE_OFFICER_ID NUMBER,CASE_DATE DATE,CASE_TIME DATE,COMMUNITY_ACTIVE_FLAG VARCHAR2(4),"
    "CREATE_INTAKE_AGY_LOC_ID VARCHAR2(24),COMM_STAFF_ID NUMBER,COMM_STATUS VARCHAR2(48),COMMUNITY_AGY_LOC_ID "
    "VARCHAR2(24),NO_COMM_AGY_LOC_ID NUMBER,COMM_STAFF_ROLE VARCHAR2(48),AGY_LOC_ID_LIST VARCHAR2(320),"
    "STATUS_REASON VARCHAR2(128),TOTAL_UNEXCUSED_ABSENCES NUMBER,REQUEST_NAME VARCHAR2(960),CREATE_DATETIME "
    "TIMESTAMP (9),CREATE_USER_ID VARCHAR2(128),MODIFY_DATETIME TIMESTAMP (9),MODIFY_USER_ID VARCHAR2(128),"
    "RECORD_USER_ID VARCHAR2(120),INTAKE_AGY_LOC_ASSIGN_DATE DATE,AUDIT_TIMESTAMP TIMESTAMP (9),AUDIT_USER_ID "
    "VARCHAR2(128),AUDIT_MODULE_NAME VARCHAR2(260),AUDIT_CLIENT_USER_ID VARCHAR2(256),AUDIT_CLIENT_IP_ADDRESS "
    "VARCHAR2(156),AUDIT_CLIENT_WORKSTATION_NAME VARCHAR2(256),AUDIT_ADDITIONAL_INFO VARCHAR2(1024),BOOKING_SEQ "
    "NUMBER,ADMISSION_REASON VARCHAR2(48)"
)


def get_schema(with_event_type=False):

    field_list = ddl.replace("TIMESTAMP (9)", "TIMESTAMP(9)").split(",")
    struct_list = []

    for field in field_list:
        field_name, field_type = field.split(" ")
        field_name = field_name.lower()
        field_type = field_type.split("(")[0].lower()

        struct_list.append(StructField(field_name, __type_for(field_type), True))

    struct_list.append(StructField("admin_hash", StringType(), True))
    struct_list.append(StructField("admin_gg_pos", StringType(), True))
    struct_list.append(StructField("admin_gg_op_ts", TimestampType(), True))
    struct_list.append(StructField("admin_event_ts", TimestampType(), True))
    if with_event_type:
        struct_list.append(StructField("event_type", StringType(), True))
    return StructType(struct_list)


def get_primary_key():
    return "offender_book_id"


# configuration
config_dict = dict(
    source_bucket="dpr-demo-development-20220916083016121000000001",
    target_bucket="dpr-demo-development-20220916083016121000000001",
    source="data/dummy/source/OFFENDERS_202209061845.json",
    target_json="data/dummy/kinesis/transac/json/",
    target_parquet="data/dummy/kinesis/transac/parquet/",
    target_final="data/dummy/database",
    schema="oms_owner",
    table="offender_bookings",
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
        + config_dict["target_json"]
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


def write_delta(config, frame):
    # Write data as DELTA TABLE
    frame.write.format("delta").mode("overwrite").save("s3://{}/".format(config["write_path"]))

    # Generate MANIFEST file for Athena/Catalog
    # deltaTable = DeltaTable.forPath(spark, "s3://{}/".format(config["path_delta"]))
    # deltaTable.generate("symlink_format_manifest")


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
            new_val = datetime.datetime.strptime(fld_val[:10], "%Y-%m-%d")
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


def show_table(table_df):
    print("##########################################")
    print("existing records:", table_df.count())
    print("example")
    table_df.select(
        col("offender_id"),
        col("in_out_status"),
        col("booking_created_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        # col("__action"),
    ).show(10)
    # .filter((col("offender_id").isin({1061, 873, 150, 127, 128, 129})))

    print("##########################################")


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    update_config()

    target_schema = get_schema()

    df = read_s3_to_df(gluecontext=glueContext, config=config_dict, key_suffix="base")
    df = add_hash_drop_tokens(frame=df, hash_fields=["after"])
    temp_rdd = df.rdd.map(lambda row: mapper(row_in=row, schema=target_schema))

    local_df_out = temp_rdd.toDF(schema=target_schema)
    # out_dyf = DynamicFrame.fromDF(local_df_out, glueContext, "out_dyf")

    # write_frame(gluecontext=glueContext, config=config_dict, frame=out_dyf)
    write_delta(config=config_dict, frame=local_df_out)
    show_table(local_df_out)


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
