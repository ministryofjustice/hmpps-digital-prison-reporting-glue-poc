from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType, Row
from pyspark.sql import Window
import datetime

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

"""
apply goldengate events log to target
    Resolution:
        Ingest goldengate events logs from parquet on s3
        apply events appropriately to target table
        commit target table as delta on s3
        
    Logical steps:
        1. Read in event log and map to target schema
        2. Read in target table
        3. Get unique key list from event log
        4. Extract Records to be considered from target
        5. Remove Records to be considered from target
        6. Identify first event in change log for new records
        7. Union steps 4 and 6
        8. Apply event log to step 7
        9. Union applied events with unconsidered records (5 and 8)
        10. Write to target
   
   Noites : To run in glue_etl docker
        1. Copy this script to /jupyter_workspace/src
        3. execute with delta support (see readme) 

        
    ToDo: 
        refactor methods into src/lib/
        enhance commentary
        resolve dynamic frame read/write catalog (requires glue catalog)
        resolve dynamic frame read write to delta tables
        

"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

BUCKET_SUFFIX = "20220906101710889000000001"
# use glue catalog True/False
USE_CATALOG = False

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
    "OFFENDER_ID number,OFFENDER_NAME_SEQ number,ID_SOURCE_CODE varchar2(48),LAST_NAME varchar2(140),NAME_TYPE "
    "varchar2(48),FIRST_NAME varchar2(140),MIDDLE_NAME varchar2(140),BIRTH_DATE date,SEX_CODE varchar2(48),"
    "SUFFIX varchar2(48),LAST_NAME_SOUNDEX varchar2(24),BIRTH_PLACE varchar2(100),BIRTH_COUNTRY_CODE varchar2(48),"
    "CREATE_DATE date,LAST_NAME_KEY varchar2(140),ALIAS_OFFENDER_ID number,FIRST_NAME_KEY varchar2(140),"
    "MIDDLE_NAME_KEY varchar2(140),OFFENDER_ID_DISPLAY varchar2(40),ROOT_OFFENDER_ID number,CASELOAD_TYPE varchar2("
    "48),MODIFY_USER_ID varchar2(128),MODIFY_DATETIME timestamp(9),ALIAS_NAME_TYPE varchar2(48),PARENT_OFFENDER_ID "
    "number,UNIQUE_OBLIGATION_FLAG varchar2(4),SUSPENDED_FLAG varchar2(4),SUSPENDED_DATE date,RACE_CODE varchar2("
    "48),REMARK_CODE varchar2(48),ADD_INFO_CODE varchar2(48),BIRTH_COUNTY varchar2(80),BIRTH_STATE varchar2(80),"
    "MIDDLE_NAME_2 varchar2(140),TITLE varchar2(48),AGE number,CREATE_USER_ID varchar2(160),LAST_NAME_ALPHA_KEY "
    "varchar2(4),CREATE_DATETIME timestamp(9),NAME_SEQUENCE varchar2(48),AUDIT_TIMESTAMP timestamp(9),AUDIT_USER_ID "
    "varchar2(128),AUDIT_MODULE_NAME varchar2(260),AUDIT_CLIENT_USER_ID varchar2(256),AUDIT_CLIENT_IP_ADDRESS "
    "varchar2(156),AUDIT_CLIENT_WORKSTATION_NAME varchar2(256),AUDIT_ADDITIONAL_INFO varchar2(1024)"
)


def get_schema(with_event_type=False, prefix=False):
    field_list = ddl.split(",")
    struct_list = []

    for field in field_list:
        field_name, field_type = field.split(" ")
        field_name = field_name.lower()
        field_type = field_type.split("(")[0].lower()
        if prefix:
            prefix = "__"
        else:
            prefix = ""
        field_name = "{}{}".format(prefix, field_name)

        struct_list.append(StructField(field_name, __type_for(field_type), True))

    struct_list.append(StructField("{}admin_hash".format(prefix), StringType(), True))
    struct_list.append(StructField("{}admin_gg_pos".format(prefix), StringType(), True))
    struct_list.append(StructField("{}admin_gg_op_ts".format(prefix), TimestampType(), True))
    struct_list.append(StructField("{}admin_event_ts".format(prefix), TimestampType(), True))
    if with_event_type:
        struct_list.append(StructField("event_type", StringType(), True))
        struct_list.append(StructField("previous_hash", StringType(), True))
    return StructType(struct_list)


def get_primary_key():
    return "offender_id"


temp_dataframe = None
# configuration
config_gg_events = dict(
    source_bucket="dpr-demo-development-{}".format(BUCKET_SUFFIX),
    source="data/dummy/kinesis/transac/parquet",
    schema="oms_owner",
    table="offenders",
)
config_target_table = dict(
    target_bucket="dpr-demo-development-{}".format(BUCKET_SUFFIX),
    target_final="data/dummy/database/",
    schema="oms_owner",
    table="offenders",
    # partition_by = ["date", "time"]
    partition_by=["part_date"],
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

    config_target_table["path_delta"] = (
            config_target_table["target_bucket"]
            + "/"
            + config_target_table["target_final"]
            + "/delta/"
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
    write glue dynamic frame to S3
        CURRENTLY THIS IS NOT WORKING WITH DELTA FORMAT
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """

    print("###### s3 writing to {}".format(config["path"]))
    gluecontext.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={
            "path": "s3://{}/".format(config["path"]),
            "partitionKeys": config["partition_by"],
        },
        format="delta",
    )


def write_delta(config, frame):
    # Write data as DELTA TABLE
    frame.write.format("delta").mode("overwrite").save("s3://{}/".format(config["path_delta"]))

    # Generate MANIFEST file for Athena/Catalog
    # deltaTable = DeltaTable.forPath(spark, "s3://{}/".format(config["path_delta"]))
    # deltaTable.generate("symlink_format_manifest")


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
    return input_dydf.toDF()


def create_empty_df(schema):
    return spark.createDataFrame(data=spark.sparkContext.emptyRDD(), schema=schema)


def load_sample_to_df(df, sample=0.01):
    """
    sample df records
    :param sample: sample decimal
    :param df: dataframe to sample
    :return: sampled dataframe
    """
    dfsample = df.sample(sample)
    print(dfsample.count())
    return dfsample


def output_df():
    target_schema = get_schema()

    temp_df = create_empty_df(target_schema)
    return temp_df


def get_schema_fields_as_dict(schema):
    """
    convert schem to dictionary
    :param schema: schema
    :return: dictionary containing schema
    """
    schema_dict = {}
    for fld in schema:
        schema_dict[fld.name] = None
    return schema_dict


def get_schema_field_type(schema, fldname):
    """
    get datatype of field name from schema
    :param schema: schema
    :param fldname: field name
    :return: datatype
    """

    for fld in schema:
        if fld.name == fldname:
            return fld.dataType


def format_field(schema, fldname, fld_val):
    """
    format field value based on schema datatypes
    :param schema: schema
    :param fldname: fieldname
    :param fld_val: field value
    :return: field value as datatype
    """
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
    """
    map rows to schema
    :param row_in: row containing unmapped record
    :param schema: schema definition
    :return: row type in correct schema format
    """

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


def convert_to_dict_list(frame):
    """
    convert all rows to dictionary list
    :param frame: dataframe
    :return: list of dictionarys containing row data
    """
    out_dict = frame.rdd.map(lambda row: row.asDict()).collect()
    return out_dict


def compare_dicts(dict_orig, row_in, key_field):
    """
    compare the two dictionary records and update original based on event type
    :param dict_orig: original dict
    :param row_in: event record
    :param key_field: record key
    :return: updated original dict
    """
    if row_in["previous_hash"] == dict_orig["admin_hash"]:
        if row_in["event_type"] == "U":
            for key in dict_orig:
                if key == "__action":
                    dict_orig[key] = "U"
                else:
                    dict_orig[key] = row_in[key]

        if row_in["event_type"] == "D":
            dict_orig["__action"] = "D"
        if row_in["event_type"] == "I":
            if dict_orig["__action"] == "D":
                for key in dict_orig:
                    if key == "__action":
                        dict_orig[key] = "U"
                    else:
                        dict_orig[key] = row_in[key]

    return dict_orig


def apply_events(row_in, key_field, event_dict):
    """
    apply the events to the row sequentially
    :param row_in: original row
    :param key_field: record key
    :param event_dict: list of events as dict
    :return: updated row
    """
    row_dict = row_in.asDict()

    tmp_dict = []
    for dct in event_dict:
        if dct[key_field] == row_dict[key_field]:
            tmp_dict.append(dct)

    for event_rec in tmp_dict:
        row_dict = compare_dicts(row_dict, event_rec, key_field)

    return Row(**row_dict)


def rename_columns(frame):
    new_column_name_list = list(map(lambda x: "__{}".format(x), frame.columns))
    return frame.toDF(*new_column_name_list)


def show_table(table_df):
    print("##########################################")
    print("existing records:", table_df.count())
    print("example")
    table_df.select(
        col("offender_id"),
        col("title"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        # col("__action"),
    ).filter((col("offender_id").isin({1061, 873, 127, 128, 129}))).show(10)

    print("##########################################")


def show_events(event_df):
    print("##########################################")
    print("event records:", event_df.count())
    event_df.select(
        col("offender_id"),
        col("title"),
        col("create_date"),
        col("admin_hash"),
        col("previous_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        col("event_type"),
    ).filter((col("offender_id").isin({1061, 873, 127, 128, 129}))).sort("offender_id", "admin_gg_pos").show(30)
    print("##########################################")


def show_bef_after_applied(df_to_consider, df_applied):
    print("##########################################")
    print("records to consider:", df_to_consider.count())
    print("example")
    df_to_consider.select(
        col("offender_id"),
        col("title"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        col("__action"),
    ).filter((col("offender_id").isin({1061, 873, 127, 128, 129}))).show(10)

    print("records applied:", df_applied.count())
    print("example")
    df_applied.select(
        col("offender_id"),
        col("title"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        col("__action"),
    ).filter((col("offender_id").isin({1061, 873, 127, 128, 129}))).show(50)

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

    temp_schema = get_schema(with_event_type=True)

    target_key = get_primary_key()
    """1. Read in event log map to schema"""
    df_event_log = read_s3_to_df(gluecontext=glueContext, config=config_gg_events)  # , key_suffix="date=2022-09-13")
    df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=temp_schema)).toDF(schema=temp_schema)

    """2. Read in target table"""
    df_table_in = read_s3_to_df(gluecontext=glueContext, config=config_target_table)
    # df_table_in = df_table_in.withColumn("status", lit(0))

    # df_event_log.show()
    # df_table_in.show()

    # show_table(df_table_in)

    """3. Get unique key list from event log"""
    df_unique_key = rename_columns(frame=df_event_log.select(target_key).distinct())

    # consider events against existing records

    show_events(df_event_log)
    """4. Extract Records to be considered from target"""
    df_to_consider = df_table_in.join(
        df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "inner"
    ).drop("__{}".format(target_key))

    # df_to_consider = df_to_consider.withColumn("__action", lit(""))
    """5. Remove Records to be considered from target"""
    df_unique_applied_key = df_to_consider.select(target_key).distinct()

    df_new_key = (
        df_unique_key.join(
            df_unique_applied_key, df_unique_applied_key[target_key] == df_unique_key["__{}".format(target_key)], "left"
        )
            .filter(col(target_key).isNull())
            .drop(target_key)
    )

    df_table_in = (
        df_table_in.join(df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "left")
            .filter(col("__{}".format(target_key)).isNull())
            .drop("__{}".format(target_key))
    )

    """6. Identify first event in change log for new records"""
    w = Window.partitionBy(target_key)
    df_primary_events = (
        df_event_log.withColumn("minpos", min("admin_gg_pos").over(w))
            .where(col("admin_gg_pos") == col("minpos"))
            .drop("minpos")
    )

    df_primary_events = df_primary_events.drop("event_type").drop("previous_hash")

    df_to_consider_2 = df_primary_events.join(
        df_new_key, df_primary_events[target_key] == df_new_key["__{}".format(target_key)], "inner"
    ).drop("__{}".format(target_key))

    """7. Union steps 4 and 6"""
    df_to_consider = df_to_consider.unionByName(df_to_consider_2)

    """8. Apply event log to step 7"""
    df_to_consider = df_to_consider.withColumn("__action", lit(""))

    action_schema = df_to_consider.schema

    df_event_log = df_event_log.sort("admin_gg_pos")

    temp_dict_list = convert_to_dict_list(df_event_log)

    df_applied = df_to_consider.rdd.map(
        lambda row: apply_events(row_in=row, key_field=target_key, event_dict=temp_dict_list)
    ).toDF(schema=action_schema)

    show_bef_after_applied(df_to_consider, df_applied)

    """9. Union applied events with unconsidered records (5 and 8)"""
    df_applied = df_applied.filter(col("__action").isin({"U", "I"})).drop("__action")
    df_table_out = df_applied.unionByName(df_table_in, allowMissingColumns=True)


    df_table_out = df_table_out.withColumn(config_target_table["partition_by"][0], col("create_date"))
    show_table(df_table_out)

    """10. Write to target"""
    write_delta(config=config_target_table, frame=df_table_out)
    # out_dyf = DynamicFrame.fromDF(df_table_out, glueContext, "out_dyf")
    # config_target_table["path"] = config_target_table["path_delta"]
    # write_frame(gluecontext=glueContext, config=config_target_table, frame=out_dyf)


if __name__ == "__main__":
    start()
