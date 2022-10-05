import json
import time

import boto3

from pyspark.sql.functions import *
from pyspark.sql.types import Row

"""
apply changes from structured to curated tables
    Resolution:
        Read structured tables as delta
        commit curated table as delta on s3

    Logical steps:
        0. Read In kinesis event trigger table 
        1. Read in domain definition table
        2. extract source definition from 1
        3. extract relevant entries from 2
        4. read sources for each target
        5. execute resolution
        6. write target
        7. trigger kinesis

   Noites : To run in glue_etl docker
        1. Copy this script to ~/jupyter_workspace/src
        3. execute with delta support (see readme) 


    ToDo: 
        refactor methods into src/lib/
        enhance commentary
        resolve dynamic frame read/write catalog (requires glue catalog)
        resolve kinesis triggering

        step 0 kinesis events dont yet exist
        step 2 writes to <domain>_<table>


"""
__author__ = "frazer.clayton@digital.justice.gov.uk"

DATABASE_NAME = "dpr-glue-catalog-development-development"
KINESIS_EVENTS_TABLES = ["offender_bookings", "offender_bookings", "anottable"]
DOMAIN_DEFINITIONS_TABLE = "domain_definitions"


def generate_process_id():
    epoch_t = t = int(time.time() * 1000)

    return epoch_t


def run_statement(active_statement, process_id):
    statement_dict = active_statement.asDict()
    res_df_array = []
    table_array = []
    # create views on tables
    for tablename in statement_dict["Dependancies"].split(","):
        table_df = read_delta_table(
            database=DATABASE_NAME, table_name=tablename)
        table_df.createOrReplaceTempView(tablename)
        table_array.append(table_df)
    sql_statement = statement_dict["Resolution"]
    res_df = spark.sql(sql_statement)
    res_df = res_df.withColumn(colName="process_id", col=lit(process_id))
    res_dict = {"target_table": statement_dict["Target"], "res_df": res_df}
    return res_dict


def get_unique_list(event_tables):
    event_tables_unique = list(set(event_tables))
    return event_tables_unique


def are_any_in_list(dep_list, event_tables):
    count = 0
    for dep_table in dep_list:
        if dep_table in event_tables:
            count = count + 1
    if count > 0:
        return True
    return False


def filter_statements(statement_row, event_tables):
    row_dict = statement_row.asDict()

    if are_any_in_list(statement_row["Dependancies"].split(","), event_tables):
        row_dict["Status"] = "ACTIVE"
    else:
        row_dict["Status"] = "INACTIVE"

    return Row(**row_dict)


def get_required_defs(domain_def_df, event_tables_unique):
    domain_def_schema = domain_def_df.schema
    df_active_statements = (
        domain_def_df.rdd.map(lambda row: filter_statements(
            statement_row=row, event_tables=event_tables_unique))
        .toDF(schema=domain_def_schema)
        .filter(col("Status") == "ACTIVE")
    )

    return df_active_statements


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


def schema_to_columns(inputDF):
    _schema_json = json.loads(inputDF.schema.json())
    column_list = []
    for field in _schema_json["fields"]:
        if field["name"] in {"before", "after"}:
            field["type"] = "string"
        column_list.append({"Name": field["name"], "Type": type_for(
            field["type"]), "Comment": "", "Parameters": {}})
    return column_list


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
        connection_options={"paths": [read_path]},
        format=file_format,
        format_options={"withHeader": True},
    )
    return input_dydf.toDF()


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


def write_delta_table(database, table_name, frame):
    """
    write out dataframe as delta format table
    :param database: database name
    :param table_name: table name
    :param frame: dataframe to write
    :return: None
    """
    # Write data as DELTA TABLE

    frame.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(get_table_location(database, table_name))


    # Generate MANIFEST file for Athena/Catalog
    # deltaTable = DeltaTable.forPath(spark, "s3://{}/".format(config["path"]))
    # deltaTable.generate("symlink_format_manifest")


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
    event_tables = KINESIS_EVENTS_TABLES
    event_tables_unique = get_unique_list(event_tables)

    domain_def_df = read_table_to_df(
        gluecontext=glueContext, database=DATABASE_NAME, table_name=DOMAIN_DEFINITIONS_TABLE, file_format="csv"
    )

    df_active_statements = get_required_defs(domain_def_df=domain_def_df, event_tables_unique=event_tables_unique)
    process_id = generate_process_id()

    for definition in df_active_statements.rdd.collect():
        ret_dict = run_statement(active_statement=definition, process_id=process_id)
        print(ret_dict["target_table"])
        ret_dict["res_df"].show()
        write_delta_table(database=DATABASE_NAME,
                          table_name=ret_dict["target_table"], frame=ret_dict["res_df"])
        update_column_list_in_glue(
            DATABASE_NAME, table_name=ret_dict["target_table"], columns=schema_to_columns(
                ret_dict["res_df"])
        )


if __name__ == "__main__":
    from pyspark.shell import spark

    start()
