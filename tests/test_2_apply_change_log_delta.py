from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

from src.apply_change_log_to_delta import update_config, config_gg_events, config_target_table, get_schema, \
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
    test_schema.fields.append(StructField("event_type", StringType(), True))
    test_schema.fields.append(StructField("previous_hash", StringType(), True))

    print(test_schema)