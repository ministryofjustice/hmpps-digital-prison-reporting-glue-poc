from time import strptime

from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType, Row
import datetime

from src.config.generic import update_config, config_gg_events, config_target_table
from src.lib.aws_io import read_s3_to_df
from src.lib.map_to_schema import mapper
from src.lib.rename import rename_columns
from src.schema.offenders import get_schema, get_primary_key

"""
apply goldengate events log to target


"""
__author__ = "frazer.clayton@digital.justice.gov.uk"


def convert_to_dict_list(frame):
    """
    convert dataframe to list of dict form
    :param frame: dataframe
    :return: list of data in dict form
    """
    out_dict = frame.rdd.map(lambda row: row.asDict()).collect()
    return out_dict


def compare_events(dict_orig, row_in, key_field):
    """
    compare events applying changes (where previous hash not match reject)
    :param dict_orig: original record in dict form
    :param row_in: event record in dict form
    :param key_field: key field to make comparison
    :return: updated dict
    """
    if row_in["previous_hash"] == dict_orig["admin_hash"]:
        if row_in["event_type"] == "U":
            for key in dict_orig:
                dict_orig[key] = row_in[key]
        if row_in["event_type"] == "D":
            dict_orig[key_field] = -9876
        if row_in["event_type"] == "I":
            if dict_orig[key_field] < 0:
                for key in dict_orig:
                    dict_orig[key] = row_in[key]

    return dict_orig


def apply_events(row_in, key_field, event_dict):
    """
    apply events in to row_in
    :param row_in: original row
    :param key_field: primary key
    :param event_dict: list of events in dict form
    :return: row with changes applied
    """
    row_dict = row_in.asDict()

    tmp_dict = []
    for dct in event_dict:
        if dct[key_field] == row_dict[key_field]:
            tmp_dict.append(dct)
    print("tmp dict len {}".format(len(tmp_dict)))

    for event_rec in tmp_dict:
        row_dict = compare_events(row_dict, event_rec, key_field)

    return Row(**row_dict)


def start():
    """
    start the processing
    :return: None
    """
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())

    update_config()

    temp_schema = get_schema(with_event_type=True)
    target_schema = get_schema()
    target_key = get_primary_key()

    df_event_log = read_s3_to_df(gluecontext=glueContext, config=config_gg_events)  # , key_suffix="date=2022-09-13")

    df_table_in = read_s3_to_df(gluecontext=glueContext, config=config_target_table)
    # df_table_in = df_table_in.withColumn("status", lit(0))

    # df_event_log.show()
    # df_table_in.show()

    df_event_log = df_event_log.rdd.map(lambda row: mapper(row_in=row, schema=temp_schema)).toDF(schema=temp_schema)

    df_unique_key = rename_columns(frame=df_event_log.select(target_key).distinct())

    df_to_consider = df_table_in.join(df_unique_key,
                                      df_table_in[target_key] == df_unique_key["__{}".format(target_key)],
                                      "inner").drop("__{}".format(target_key))

    print("records to consider:", df_to_consider.count())
    df_event_log.sort("admin_gg_pos")
    temp_dict_list = convert_to_dict_list(df_event_log)
    print(temp_dict_list[0])

    df_applied = df_to_consider.rdd.map(
        lambda row: apply_events(row_in=row, key_field=target_key, event_dict=temp_dict_list)).toDF(
        schema=target_schema)
    print("records applied:", df_applied.count())
    df_applied.show(1)

    # new_column_name_list= list(map(lambda x: "__{}".format(x), df_event_log.columns))
    # df_event_log = df_event_log.toDF(*new_column_name_list)

    # df_joined = df_table_in.join(df_event_log,df_table_in["offender_id"] ==  df_event_log["__offender_id"],"left")


if __name__ == "__main__":
    start()
