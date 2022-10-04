import datetime

from src.apply_to_domains import get_required_defs, run_statement, get_unique_list
from pyspark.sql.functions import col
from pyspark.sql.types import Row

KINESIS_EVENTS_TABLES = ["offender_bookings", "offender_bookings", "anottable"]


def run_statement(spark_session, active_statement):
    statement_dict = active_statement.asDict()
    res_df_array = []
    table_array = []
    # create views on tables
    for tablename in statement_dict["Dependancies"].split(","):
        table_df = read_table(spark_session=spark_session,
                              table_name=tablename)
        table_df.createOrReplaceTempView(tablename)
        table_array.append(table_df)
    sql_statement = statement_dict["Resolution"]
    res_df = spark_session.sql(sql_statement)
    res_dict = {"target_table": statement_dict["Target"], "res_df": res_df}
    return res_dict


def read_table(spark_session, table_name):
    read_offenders = "./tests/data/dummy/database/oms_owner/offenders_curated/"
    read_offender_bookings = "./tests/data/dummy/database/oms_owner/offender_bookings_curated/"

    if table_name == "offenders":
        return spark_session.read.parquet(read_offenders)

    if table_name == "offender_bookings":
        return spark_session.read.parquet(read_offender_bookings)


def test_apply_to_domain(spark_session):
    event_tables = ["offender_bookings", "offender_bookings", "anottable"]
    domain_def = "./tests/data/dummy/database/domain_definitions/"

    event_tables = KINESIS_EVENTS_TABLES
    event_tables_unique = get_unique_list(event_tables)
    assert event_tables_unique.sort(
    ) == ["offender_bookings", "anottable"].sort()

    domain_def_df = spark_session.read.option("header", True).csv(domain_def)

    df_active_statements = get_required_defs(
        domain_def_df=domain_def_df, event_tables_unique=event_tables_unique)

    for definition in df_active_statements.rdd.collect():

        ret_dict = run_statement(spark_session=spark_session, active_statement=definition)
        if ret_dict["target_table"] == 'domain2_book_off':

            assert ret_dict["res_df"].select(col("offender_book_id"),
                                             col("offender_name"),
                                             col("in_out_status"),
                                             ).filter(col("offender_book_id").isin(93)).collect()[0] \
                   == Row(offender_book_id=93, offender_name='David Martin', in_out_status='OUT')
            assert ret_dict["res_df"].select(col("offender_book_id"),
                                             col("offender_name"),
                                             col("in_out_status"),
                                             ).filter(col("offender_book_id").isin(88)).collect()[0] \
                   == Row(offender_book_id=88, offender_name='Patrick Murphy', in_out_status='IN')
        if ret_dict["target_table"] == 'domain1_off_book':

            assert ret_dict["res_df"].select(col("offender_id"),
                                             col("offender_name"),
                                             col("in_out_status"),
                                             col("booking_begin_date"),
                                             col("booking_end_date"),
                                             ).filter(col("offender_id").isin(9)).collect()[0] \
                   == Row(offender_id=9, offender_name='David Martin',
                          in_out_status='OUT', booking_begin_date=datetime.date(2019, 5, 7),
                          booking_end_date=datetime.date(2022, 9, 24))
            assert ret_dict["res_df"].select(col("offender_id"),
                                             col("offender_name"),
                                             col("in_out_status"),
                                             col("booking_begin_date"),
                                             col("booking_end_date"),
                                             ).filter(col("offender_id").isin(4)).collect()[0] \
                   == Row(offender_id=4, offender_name='Patrick Murphy',
                          in_out_status='IN', booking_begin_date=datetime.date(2011, 9, 29),
                          booking_end_date=None)

