import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, Row

from src.apply_change_log_to_delta import \
    get_primary_key, mapper, show_table, rename_columns, show_events, get_distinct_column_values_from_df, \
    format_table_name, update_schema, apply_events, convert_to_dict_list

test_schema = StructType(
    [StructField('offender_id', IntegerType(), True), StructField('offender_name_seq', IntegerType(), True),
     StructField('id_source_code', StringType(), True), StructField('last_name', StringType(), True),
     StructField('name_type', StringType(), True), StructField('first_name', StringType(), True),
     StructField('middle_name', StringType(), True), StructField('birth_date', DateType(), True),
     StructField('sex_code', StringType(), True), StructField('suffix', StringType(), True),
     StructField('last_name_soundex', StringType(), True), StructField('birth_place', StringType(), True),
     StructField('birth_country_code', StringType(), True), StructField('create_date', DateType(), True),
     StructField('last_name_key', StringType(), True), StructField('alias_offender_id', IntegerType(), True),
     StructField('first_name_key', StringType(), True), StructField('middle_name_key', StringType(), True),
     StructField('offender_id_display', StringType(), True), StructField('root_offender_id', IntegerType(), True),
     StructField('caseload_type', StringType(), True), StructField('modify_user_id', StringType(), True),
     StructField('modify_datetime', TimestampType(), True), StructField('alias_name_type', StringType(), True),
     StructField('parent_offender_id', IntegerType(), True), StructField('unique_obligation_flag', StringType(), True),
     StructField('suspended_flag', StringType(), True), StructField('suspended_date', DateType(), True),
     StructField('race_code', StringType(), True), StructField('remark_code', StringType(), True),
     StructField('add_info_code', StringType(), True), StructField('birth_county', StringType(), True),
     StructField('birth_state', StringType(), True), StructField('middle_name_2', StringType(), True),
     StructField('title', StringType(), True), StructField('age', IntegerType(), True),
     StructField('create_user_id', StringType(), True), StructField('last_name_alpha_key', StringType(), True),
     StructField('create_datetime', TimestampType(), True), StructField('name_sequence', StringType(), True),
     StructField('audit_timestamp', TimestampType(), True), StructField('audit_user_id', StringType(), True),
     StructField('audit_module_name', StringType(), True), StructField('audit_client_user_id', StringType(), True),
     StructField('audit_client_ip_address', StringType(), True),
     StructField('audit_client_workstation_name', StringType(), True),
     StructField('audit_additional_info', StringType(), True), StructField('admin_hash', StringType(), True),
     StructField('admin_gg_pos', StringType(), True), StructField('admin_gg_op_ts', TimestampType(), True),
     StructField('admin_event_ts', TimestampType(), True), StructField('event_type', StringType(), True),
     StructField('previous_hash', StringType(), True), StructField('table', StringType(), True),
     StructField('schema', StringType(), True)])


# def test_update_config(spark_session):
#     config_gg_events["source_bucket"] = "./tests"
#     config_target_table["target_bucket"] = "./tests"
#     update_config()
#
#     assert config_target_table["path"] == "./tests/data/dummy/database/oms_owner/offenders"
#     assert config_gg_events["path"] == './tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders'
#     test_schema.fields.append(StructField("event_type", StringType(), True))
#     test_schema.fields.append(StructField("previous_hash", StringType(), True))
#
#     print(test_schema)


def test_get_table_list(spark_session):
    read_logs = "./tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders/"

    df_event_log_in = spark_session.read.parquet(read_logs)

    table_list = get_distinct_column_values_from_df(frame=df_event_log_in, column="table")

    assert table_list == ['OMS_OWNER.OFFENDERS']

    target_table = format_table_name(table_list[0])
    target_key = get_primary_key(table=target_table)

    assert target_table == "offenders"
    assert target_key == "offender_id"


def test_update_schema(spark_session):
    read_table = "tests/data/dummy/database/oms_owner/offenders/"
    read_logs = "./tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders/"

    df_event_log_in = spark_session.read.parquet(read_logs)

    df_table_in = spark_session.read.parquet(read_table)

    temp_schema = update_schema(schema=df_table_in.schema, with_event_type=True)

    assert temp_schema == test_schema


def test_mapper(spark_session):
    read_table = "tests/data/dummy/database/oms_owner/offenders/"
    read_logs = "./tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders/"

    df_event_log = spark_session.read.parquet(read_logs)

    df_table_in = spark_session.read.parquet(read_table)

    temp_schema = update_schema(schema=df_table_in.schema, with_event_type=True)

    df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=temp_schema)).toDF(schema=temp_schema)

    # df_event_log.select(col("last_name")).filter(col("offender_id").isin({149}) &col("event_type").isin({"U"})).show()
    assert df_event_log.select("last_name").filter(
        col("offender_id").isin({149}) & col("event_type").isin({"U"})
    ).collect()[0] == Row(last_name='Lightfoot')


def test_apply_events(spark_session):
    read_table = "tests/data/dummy/database/oms_owner/offenders/"
    read_logs = "./tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders/"

    df_event_log = spark_session.read.parquet(read_logs)
    table_list = get_distinct_column_values_from_df(frame=df_event_log, column="table")

    target_table = format_table_name(table_list[0])
    target_key = get_primary_key(table=target_table)
    df_table_in = spark_session.read.parquet(read_table)

    temp_schema = update_schema(schema=df_table_in.schema, with_event_type=True)

    df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=temp_schema)).toDF(schema=temp_schema)

    df_unique_key = rename_columns(frame=df_event_log.select(target_key).distinct())

    assert df_unique_key.select("__offender_id").filter(col("__offender_id") == 144).collect()[0] == Row(
        __offender_id=144)

    assert df_unique_key.count() == 1951

    df_to_consider = df_table_in.join(
        df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "inner"
    ).drop("__{}".format(target_key))

    """5. Remove Records to be considered from target"""

    df_to_remain = (
        df_table_in.join(df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "left")
            .filter(col("__{}".format(target_key)).isNull())
            .drop("__{}".format(target_key))
    )

    assert df_to_consider.count() + df_to_remain.count() == df_table_in.count()

    df_to_consider = df_to_consider.withColumn("__action", lit(""))

    action_schema = df_to_consider.schema

    df_event_log = df_event_log.sort("admin_gg_pos")

    temp_dict_list = convert_to_dict_list(df_event_log)

    print(target_key)
    df_applied = df_to_consider.rdd.map(
        lambda row: apply_events(row_in=row, key_field=target_key, event_dict=temp_dict_list)
    ).toDF(schema=action_schema)

    assert df_applied.count() == df_to_consider.count()
    assert df_applied.select(col("last_name"),
                             col("title"),
                             col("offender_id"),
                             col("__action"),
                             ).filter(col("offender_id").isin(144)).collect()[0] \
           == Row(last_name='Byrne', title='Mr', offender_id=144, __action='D')

    df_applied.select(col("last_name"),
                             col("title"),
                             col("offender_id"),
                             col("__action"),
                             ).show(100)

    # .filter(col("__action").isin({'U','D'}))
