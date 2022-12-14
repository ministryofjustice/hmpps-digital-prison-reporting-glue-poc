from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import json
import datetime
import boto3
from pyspark.sql.types import *

DATABASE_NAME = "dpr-glue-catalog-development-development"
TABLE_NAMES = [
    "offender_bookings",
    "offender_bookings_curated",
    "offender_bookings_orig",
    "offenders",
    "offenders_curated",
    "offenders_orig",
]
TABLE_NAMES = ["offender_bookings"]

table_pks = {
    "offender_bookings": "offender_book_id",
    "offender_bookings_curated": "offender_book_id",
    "offender_bookings_orig": "offender_book_id",
    "offenders": "offender_id",
    "offenders_curated": "offender_id",
    "offenders_orig": "offender_id",
}

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
    "int": lambda t: IntegerType(),
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


def build_file_struct_schema(field_list):
    struct_list = []

    for field in field_list:
        field_name, field_type = field.split(" ")
        field_name = field_name.lower()
        field_type = field_type.split("(")[0].lower()

        struct_list.append(StructField(
            field_name, __type_for(field_type), True))

    return StructType(struct_list)


glue_datatypes = {
    "integer": "int",
    "date": "date",
    "timestamp": "timestamp",
    "string": "string",
}


def type_for(datatype):
    """
    return types for field
    """
    return glue_datatypes.get(datatype, "string")


def get_table_location(database, table_name):
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    return table_def["Table"]["StorageDescriptor"]["Location"]


def get_table_columns(database, table_name):
    field_list = []
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    for field in table_def["Table"]["StorageDescriptor"]["Columns"]:
        field_list.append(field["Name"] + " " + field["Type"])

    for field_id in field_list:
        print(field_id)
    structured_schema = build_file_struct_schema(field_list)
    print(structured_schema)


def get_table_pk(database, table_name):
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    print(table_def["Table"]["Parameters"]["PK"])


def update_column_list_in_glue(database, table_name, columns, pk=None):
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)
    table_parameters = {
        "classification": "delta",
        "EXTERNAL": "TRUE",
    }

    if pk is not None:
        table_parameters['PK'] = pk

    table_def["Table"]["StorageDescriptor"]["Columns"] = columns
    table_def["Table"]["TableType"] = "EXTERNAL_TABLE"
    table_def["Table"]["Parameters"] = table_parameters

    table_def["Table"].pop("DatabaseName")
    table_def["Table"].pop("CatalogId")
    table_def["Table"].pop("CreateTime")
    table_def["Table"].pop("UpdateTime")
    table_def["Table"].pop("CreatedBy")
    table_def["Table"].pop("IsRegisteredWithLakeFormation")

    boto_glue.update_table(DatabaseName=database,
                           TableInput=table_def["Table"])


def read_delta_table(database, table_name):
    """
    Read in table as delta into dataframe
    :param database: database_name
    :param table_name: table_name
    :return: dataframe
    """

    frame = spark.read.format("delta").load(
        get_table_location(database, table_name))
    return frame


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


def schema_to_columns(inputDF):
    _schema_json = json.loads(inputDF.schema.json())
    column_list = []
    for field in _schema_json["fields"]:
        if field["name"] in {"before", "after"}:
            field["type"] = "string"
        column_list.append({"Name": field["name"], "Type": type_for(
            field["type"]), "Comment": "", "Parameters": {}})
    return column_list


def start():
    glueContext = GlueContext(SparkContext.getOrCreate())

    # inputDF = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
    #   "paths": ["s3://{}".format(read_path)]}, format="json")
    for table_name in TABLE_NAMES:
        print(get_table_location(DATABASE_NAME, table_name))
        # inputDF = read_table(glueContext, DATABASE_NAME, table_name)
        # inputDF = read_delta_table(DATABASE_NAME, table_name)
        # update_column_list_in_glue(database=DATABASE_NAME, table_name=table_name,
        #                          columns=schema_to_columns(inputDF), pk=table_pks[table_name])
        # print(inputDF.schema)
        get_table_columns(DATABASE_NAME, table_name)
        get_table_pk(DATABASE_NAME, table_name)


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
