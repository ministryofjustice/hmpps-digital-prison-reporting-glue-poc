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
        11. trigger kinesis

   Noites : To run in glue_etl docker
        1. Copy this script to ~/jupyter_workspace/src
        3. execute with delta support (see readme) 


    ToDo: 
        refactor methods into src/lib/
        enhance commentary
        resolve dynamic frame read/write catalog (requires glue catalog)
        resolve kinesis triggering
        
        step 1 currently reads from <table>_orig
        step 10 writes to <table>


"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

DATABASE_NAME = "dpr-glue-catalog-development-development"
GG_TRANSAC_EVENTS_TABLE = "goldengate_transac_parquet_logs"

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
    """
    get table data location from glue catalogue
    :param database: database name
    :param table_name: table name
    :return: full path to data
    """
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    return table_def["Table"]["StorageDescriptor"]["Location"]


def format_table_name(table_name):
    """
    format table name from <UPPERCASE SCHEMA>.<UPPERCASE TABLE NAME> to <lowercase table name>
    :param table_name: SCHEMA.TABLE_NAME
    :return: table_name
    """
    return table_name.split(".")[1].lower()


def update_schema(schema, with_event_type=False, prefix=False):
    """
    update schema to add process fields
    :param schema: schema
    :param with_event_type: true or false
    :param prefix: for future use
    :return: Structtype schema
    """
    struct_list = schema.fields
    if with_event_type:
        struct_list.append(StructField("event_type", StringType(), True))
        struct_list.append(StructField("previous_hash", StringType(), True))
        struct_list.append(StructField("table", StringType(), True))
        struct_list.append(StructField("schema", StringType(), True))

    return StructType(struct_list)


def get_primary_key(table):
    """
    return primary key of dataset
    :param table: table name
    :return: primary key
    """
    if table == "offenders":
        return "offender_id"
    if table == "offender_bookings":
        return "offender_book_id"


temp_dataframe = None


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
    """
    Read in table as delta into dataframe
    :param database: database_name
    :param table_name: table_name
    :return: dataframe
    """

    frame = spark.read.format("delta").load(get_table_location(database, table_name))
    return frame


def write_delta_table(database, table_name, frame):
    """
    write out dataframe as delta format table
    :param database: database name
    :param table_name: table name
    :param frame: dataframe to write
    :return: None
    """
    # Write data as DELTA TABLE
    frame.write.format("delta").mode("overwrite").save(get_table_location(database, table_name))

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
        if fld_name.lower() in new_row_dict:
            new_row_dict[fld_name.lower()] = format_field(
                schema=schema, fldname=fld_name.lower(), fld_val=row_dict[fld_name]
            )
    new_row_dict["admin_hash"] = row_in["after_hash"]

    new_row_dict["previous_hash"] = row_in["before_hash"]
    new_row_dict["admin_gg_pos"] = row_in["pos"]
    new_row_dict["admin_gg_op_ts"] = format_field(schema=schema, fldname="admin_gg_op_ts", fld_val=row_in["op_ts"])
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
    if 1 == 1:  # row_in["previous_hash"] == dict_orig["admin_hash"]:
        if row_in["event_type"] == "U":
            for key in dict_orig:
                if key == "__action":
                    dict_orig[key] = "U"
                else:
                    dict_orig[key] = row_in[key]

        if row_in["event_type"] == "D":
            dict_orig["__action"] = "D"
        if row_in["event_type"] == "I":
            for key in dict_orig:
                if key == "__action":
                    dict_orig[key] = "I"
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
    """
    select distinct values from column in frame
    :param frame: dataframe
    :param column: column of interest
    :return: unique list
    """
    out_list = []

    row_array = frame.select(column).distinct().collect()
    for _row in row_array:
        out_list.append(_row[column])

    return out_list


def rename_columns(frame):
    """
    rename columns in frame (prefix with __)
    :param frame: dataframe
    :return: dataframe with renamed columns
    """
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


def trigger_kinesis_event(table_list):
    """
    trigger a message about event to kinesis
    :param table_list: list of tables
    :return: None
    """
    for table in table_list:
        print("Kinesis TX for {}".format(table))


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    """0. Read In event log and get tables to be considers"""

    df_event_log_in = read_table_to_df(
        gluecontext=glueContext, database=DATABASE_NAME, table_name=GG_TRANSAC_EVENTS_TABLE
    )

    table_list = get_distinct_column_values_from_df(frame=df_event_log_in, column="table")

    """ loop thru target tables"""
    for target_table_name in table_list:
        print(target_table_name)

        df_event_log = df_event_log_in.filter(col("table") == target_table_name)

        """1. Read in target table and extract schema"""
        target_table = format_table_name(target_table_name)
        target_key = get_primary_key(table=target_table)
        target_table_orig = target_table + "_orig"
        print(target_table)
        df_table_in = read_delta_table(database=DATABASE_NAME, table_name=target_table_orig)

        temp_schema = update_schema(schema=df_table_in.schema, with_event_type=True)

        """2. map event log to target schema"""

        df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=temp_schema)).toDF(schema=temp_schema)

        """3. Get unique key list from event log"""
        df_unique_key = rename_columns(frame=df_event_log.select(target_key).distinct())

        # consider events against existing records

        """4. Extract Records to be considered from target"""
        df_to_consider = df_table_in.join(
            df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "inner"
        ).drop("__{}".format(target_key))

        """5. Remove Records to be considered from target"""

        df_to_remain = (
            df_table_in.join(df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "left")
            .filter(col("__{}".format(target_key)).isNull())
            .drop("__{}".format(target_key))
        )

        """6. Identify first event in change log for new records"""
        df_unique_applied_key = df_to_consider.select(target_key).distinct()

        df_new_events_key = (
            df_unique_key.join(
                df_unique_applied_key,
                df_unique_applied_key[target_key] == df_unique_key["__{}".format(target_key)],
                "left",
            )
            .filter(col(target_key).isNull())
            .drop(target_key)
        )
        w = Window.partitionBy(target_key)
        df_primary_events = (
            df_event_log.withColumn("minpos", min("admin_gg_pos").over(w))
            .where(col("admin_gg_pos") == col("minpos"))
            .drop("minpos")
        )

        # drop process only fields from primary events
        df_primary_events = df_primary_events.drop("event_type").drop("previous_hash").drop("table").drop("schema")

        df_to_consider_2 = df_primary_events.join(
            df_new_events_key, df_primary_events[target_key] == df_new_events_key["__{}".format(target_key)], "inner"
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

        # show_bef_after_applied(df_to_consider, df_applied)

        """9. Union applied events with unconsidered records (5 and 8)"""
        # only consider upsert records

        df_applied = df_applied.filter(col("__action").isin({"U", "I"})).drop("__action")
        df_table_out = df_applied.unionByName(df_to_remain, allowMissingColumns=True)

        # df_table_out = df_table_out.withColumn(config_target_table["partition_by"][0], col("create_date"))

        """10. Write to target"""
        write_delta_table(database=DATABASE_NAME, table_name=target_table, frame=df_table_out)

        # show_table(df_table_in)
        # show_events(df_event_log)
        # show_table(df_table_out)
    """11. trigger kinesis"""
    trigger_kinesis_event(table_list=table_list)


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
