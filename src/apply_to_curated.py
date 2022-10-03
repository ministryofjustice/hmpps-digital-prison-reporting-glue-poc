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
apply changes from structured to curated tables
    Resolution:
        Read structured tables as delta
        commit curated table as delta on s3

    Logical steps:
        0. Read In structured table
        1. apply appropriate changes
        2. Write to target
        3. trigger kinesis

   Noites : To run in glue_etl docker
        1. Copy this script to ~/jupyter_workspace/src
        3. execute with delta support (see readme) 


    ToDo: 
        refactor methods into src/lib/
        enhance commentary
        resolve dynamic frame read/write catalog (requires glue catalog)
        resolve kinesis triggering
        
        step 0 currently reads from <table>
        step 2 writes to <table>_curated


"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

DATABASE_NAME = "dpr-glue-catalog-development-development"
STRUCTURED_TABLES = ["offenders", "offender_bookings"]


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
    ).show(10, truncate=False)
    # .filter((col("offender_id").isin(records_of_interest)))
    print("##########################################")


def trigger_kinesis_event(table_list):
    """
    trigger a message about event to kinesis
    :param table_list: list of tables
    :return: None
    """
    for table in table_list:
        print("Kinesis TX for {}".format(table))


def apply_changes(table_in_df):
    """
    apply changes to dataframe
    :param table_in_df: dataframe
    :return: changed dataframe
    """
    table_out_df = table_in_df

    return table_out_df


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    """ loop thru structured tables"""
    for table_name in STRUCTURED_TABLES:
        """0. Read In structured table"""
        source_table = table_name
        target_table = "{}_curated".format(table_name)

        df_table_in = read_delta_table(database=DATABASE_NAME, table_name=source_table)

        """1. apply appropriate changes"""
        df_table_out = apply_changes(table_in_df=df_table_in)

        """2. Write to target"""
        write_delta_table(database=DATABASE_NAME, table_name=target_table, frame=df_table_out)

    """3. trigger kinesis"""
    trigger_kinesis_event(table_list=STRUCTURED_TABLES)


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
