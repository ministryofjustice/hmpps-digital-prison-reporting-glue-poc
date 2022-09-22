import boto3

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
        0. Read In event log and get tables to be considers
        1. Read in target table and extract schema
        2. map event log to target schema
        3. Get unique key list from event log
        4. Extract Records to be considered from target
        5. Remove Records to be considered from target
        6. Identify first event in change log for new records
        7. Union steps 4 and 6
        8. Apply event log to step 7 sequentially to allow for multiple events on same record
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

DATABASE_NAME = "dpr-glue-catalog-development-development"
GG_TRANSAC_EVENTS_TABLE = "offenders_orig"

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


records_of_interest = {1061, 873, 141, 150, 127, 128, 129}


def get_table_location(database, table_name):
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    return table_def["Table"]["StorageDescriptor"]["Location"]


def format_table_name(table_name):
    return table_name.split(".")[1].lower()


def update_schema(schema, with_event_type=False, prefix=False):
    struct_list = schema.fields
    if with_event_type:
        struct_list.append(StructField("event_type", StringType(), True))
        struct_list.append(StructField("previous_hash", StringType(), True))
        struct_list.append(StructField("table", StringType(), True))
        struct_list.append(StructField("schema", StringType(), True))

    return StructType(struct_list)


def get_primary_key():
    return "offender_id"


temp_dataframe = None


def get_target_table_name(gg_table_name):
    _tt_name = gg_table_name.split(".")[1].lower()
    return _tt_name


def write_catalog(gluecontext, config, frame):
    """
    write output using glue catalog
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param frame: Glue Dynamic Frame
    :return:None
    """
    additionaloptions = {"enableUpdateCatalog": True,
                         "partitionKeys": config["partition_by"]}

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


def read_table_to_df(gluecontext, database, table_name, file_format="parquet"):
    """
    Read from S3 into Dataframe
    :param gluecontext: Glue context
    :param config: configuration dictionary
    :param key_suffix: suffix to be added to path
    :return: spark dataframe
    """
    read_path = get_table_location(database, table_name)

    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"path": read_path},
        format=file_format,
    )
    return input_dydf.toDF()


def read_delta_table(database, table_name):
    # Write data as DELTA TABLE

    frame = spark.read.format("delta").load(
        get_table_location(database, table_name))
    return frame

    # Generate MANIFEST file for Athena/Catalog
    # deltaTable = DeltaTable.forPath(spark, "s3://{}/".format(config["path_delta"]))
    # deltaTable.generate("symlink_format_manifest")


def write_delta_table(database, table_name, frame):
    # Write data as DELTA TABLE
    frame.write.format("delta").mode("overwrite").save(
        get_table_location(database, table_name))

    # Generate MANIFEST file for Athena/Catalog
    # deltaTable = DeltaTable.forPath(spark, "s3://{}/".format(config["path"]))
    # deltaTable.generate("symlink_format_manifest")


def read_s3_to_df(gluecontext, config, key_suffix=None, file_format="parquet"):
    """
    Read from S3 into Dataframe
    :param file_format: file format
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
            new_val = datetime.datetime.strptime(
                fld_val[:26], "%Y-%m-%d %H:%M:%S.%f")
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
        if fld_name.lower() in new_row_dict:
            new_row_dict[fld_name.lower()] = format_field(
                schema=schema, fldname=fld_name.lower(), fld_val=row_dict[fld_name]
            )
    new_row_dict["admin_hash"] = row_in["after_hash"]

    new_row_dict["previous_hash"] = row_in["before_hash"]
    new_row_dict["admin_gg_pos"] = row_in["pos"]
    new_row_dict["admin_gg_op_ts"] = format_field(
        schema=schema, fldname="admin_gg_op_ts", fld_val=row_in["op_ts"])
    new_row_dict["admin_event_ts"] = datetime.datetime.now()
    new_row_dict["event_type"] = row_in["op_type"]
    new_row_dict["table"] = row_in["table"].split(".")[1]
    new_row_dict["schema"] = row_in["table"].split(".")[0]
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


def get_distinct_column_values_from_df(frame, column):
    out_list = []

    row_array = frame.select(column).distinct().collect()
    for _row in row_array:
        out_list.append(_row[column])

    return out_list


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
        col("first_name"),
        col("last_name"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        # col("__action"),
    ).filter((col("offender_id").isin(records_of_interest))).show(10, truncate=False)

    print("##########################################")


def show_events(event_df):
    print("##########################################")
    print("event records:", event_df.count())
    event_df.select(
        col("table"),
        col("offender_id"),
        col("create_date"),
        col("admin_hash"),
        col("previous_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        col("event_type"),
    ).filter((col("offender_id").isin(records_of_interest))).sort("offender_id", "admin_gg_pos").show(
        30, truncate=False
    )
    print("##########################################")


def show_bef_after_applied(df_to_consider, df_applied):
    print("##########################################")
    print("records to consider:", df_to_consider.count())
    print("example")
    df_to_consider.select(
        col("offender_id"),
        col("title"),
        col("first_name"),
        col("last_name"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        col("__action"),
    ).filter((col("offender_id").isin(records_of_interest))).show(10)

    print("records applied:", df_applied.count())
    print("example")
    df_applied.select(
        col("offender_id"),
        col("title"),
        col("first_name"),
        col("last_name"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
        col("__action"),
    ).filter((col("offender_id").isin(records_of_interest))).show(50)

    print("##########################################")


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    """0. Read In event log and get tables to be considers"""

    target_table = "offender_booking"
    target_table_orig = target_table + "_orig"
    print(target_table)
    df_table_in = read_delta_table(
        database=DATABASE_NAME, table_name=GG_TRANSAC_EVENTS_TABLE)

    df_table_in.select(
        col("offender_id"),
        col("title"),
        col("first_name"),
        col("last_name"),
        col("create_date"),
        col("admin_hash"),
        col("admin_gg_pos"),
        col("admin_event_ts"),
    ).sort("offender_id").show(20)
    # .filter(col("offender_id").isin(127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145))


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
