from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import json
import datetime
import boto3
from pyspark.sql.types import StringType, BooleanType, DateType, TimestampType

DATABASE_NAME = "dpr-glue-catalog-development-development"
TABLE_NAMES = [
    "offender_bookings",
    "offender_bookings_curated",
    "offender_bookings_orig",
    "offenders",
    "offenders_curated",
    "offenders_orig",
]

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


def update_column_list_in_glue(database, table_name, columns):
    boto_glue = boto3.client("glue")
    table_def = boto_glue.get_table(DatabaseName=database, Name=table_name)

    table_def["Table"]["StorageDescriptor"]["Columns"] = columns
    table_def["Table"]["TableType"] = "EXTERNAL_TABLE"
    table_def["Table"]["Parameters"] = {
        "classification": "delta",
        "EXTERNAL": "TRUE",
    }

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
        # inputDF = read_table(glueContext, DATABASE_NAME, TABLE_NAME)
        inputDF = read_delta_table(DATABASE_NAME, table_name)
        update_column_list_in_glue(
            DATABASE_NAME, table_name, schema_to_columns(inputDF))


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
