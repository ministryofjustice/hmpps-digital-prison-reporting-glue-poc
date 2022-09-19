from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

from src.apply_change_log_delta import update_config, config_gg_events, config_target_table, get_schema, \
    get_primary_key, mapper, show_table, rename_columns, show_events

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
     StructField('previous_hash', StringType(), True)])


def test_update_config(spark_session):
    config_gg_events["source_bucket"] = "./tests"
    config_target_table["target_bucket"] = "./tests"
    update_config()

    assert config_target_table["path"] == "./tests/data/dummy/database/oms_owner/offenders"
    assert config_gg_events["path"] == './tests/data/dummy/kinesis/transac/parquet/oms_owner/offenders'

    temp_schema = get_schema(with_event_type=True)

    target_key = get_primary_key()

    assert temp_schema == test_schema
    assert target_key == "offender_id"

    df_event_log = spark_session.read.parquet(config_gg_events["path"])  # , key_suffix="date=2022-09-13")
    df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=temp_schema)).toDF(schema=temp_schema)

    df_table_in = spark_session.read.parquet(config_target_table["path"])

    show_table(df_table_in)
    show_events(df_event_log)

    """3. Get unique key list from event log"""
    df_unique_key = rename_columns(frame=df_event_log.select(target_key).distinct())



    """4. Extract Records to be considered from target"""
    df_to_consider = df_table_in.join(
        df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "inner"
    ).drop("__{}".format(target_key))

    """5. Remove Records to be considered from target"""

    df_to_ignore = df_table_in.join(
        df_unique_key, df_table_in[target_key] == df_unique_key["__{}".format(target_key)], "left"
    ).filter(col("__{}".format(target_key)).isNull()).drop("__{}".format(target_key))

    show_table(df_to_consider)
    show_table(df_to_ignore)


    """6. Identify first event in change log for new records"""
    df_unique_applied_key = df_to_consider.select(target_key).distinct()

    df_new_key = (
        df_unique_key.join(
            df_unique_applied_key, df_unique_applied_key[target_key] == df_unique_key["__{}".format(target_key)], "left"
        )
        .filter(col(target_key).isNull())
        .drop(target_key)
    )


    w = Window.partitionBy(target_key)
    df_primary_events = (
        df_event_log.withColumn("minpos", min("admin_gg_pos").cast(IntegerType()).over(w))
        .where(col("admin_gg_pos").cast(IntegerType()) == col("minpos"))
        .drop("minpos")
    )

    df_primary_events = df_primary_events.drop("event_type").drop("previous_hash")

    df_to_consider_2 = df_primary_events.join(
        df_new_key, df_primary_events[target_key] == df_new_key["__{}".format(target_key)], "inner"
    ).drop("__{}".format(target_key))

    show_table(df_to_consider_2)