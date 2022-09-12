from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import json

config_dict = dict(
    source_bucket="dpr-demo-development-20220906101710889000000001",
    target_bucket="dpr-demo-development-20220906101710889000000001",
    source="data/dummy/source/OFFENDERS_202209061845.json",
    target_json="data/dummy/kinesis/transac/json/",
    target_parquet="data/dummy/kinesis/transac/parquet/",
    schema="oms_owner",
    table="offenders",
)

output_path = (
        config_dict["target_bucket"]
        + "/"
        + config_dict["target_json"]
        + "/"
        + config_dict["schema"]
        + "/"
        + config_dict["table"]
)

glueContext = GlueContext(SparkContext.getOrCreate())
inputDF_i = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
    "paths": ["s3://{}/inserts/".format(output_path)]}, format="json")
local_df_i = inputDF_i.toDF()

local_df_i = local_df_i.withColumn("after_hash", hash("after")).drop(col("tokens"))

local_df_i.select(col("table"),
                  col("op_type"),
                  col("op_ts"),
                  col("current_ts"),
                  col("pos"),
                  col("after.offender_id"),
                  col("after_hash"),
                  col("after.first_name"),
                  col("after.last_name"),
                  col("after.title")
                  ).show(3)

inputDF_u = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
    "paths": ["s3://{}/updates/".format(output_path)]}, format="json")
local_df_u = inputDF_u.toDF()

local_df_u = local_df_u.withColumn("after_hash", hash("after")).withColumn("before_hash", hash("before")).drop(
    col("tokens"))

local_df_u.select(col("table"),
                  col("op_type"),
                  col("op_ts"),
                  col("current_ts"),
                  col("pos"),
                  col("after.offender_id"),
                  col("after_hash"),
                  col("before_hash"),
                  col("after.first_name"),
                  col("after.last_name"),
                  col("after.title")
                  ).show(3)

inputDF_d = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
    "paths": ["s3://{}/deletes/".format(output_path)]}, format="json")
local_df_d = inputDF_d.toDF()

local_df_d = local_df_d.withColumn("before_hash", hash("before")).drop(col("tokens"))

local_df_d.select(col("table"),
                  col("op_type"),
                  col("op_ts"),
                  col("current_ts"),
                  col("pos"),
                  col("before.offender_id"),
                  col("before_hash"),
                  col("before"),  # .offender_id"),
                  # col("before.first_name"),
                  # col("before.last_name"),
                  # col("before.title")
                  ).show(3)

local_df_out = local_df_i.unionByName(local_df_u, allowMissingColumns=True).unionByName(local_df_d,
                                                                                        allowMissingColumns=True)

local_df_out.select(col("table"),
                    col("op_type"),
                    col("pos"),
                    col("before.offender_id"),
                    col("after.offender_id"),
                    col("before_hash"),
                    col("after_hash"),

                    ).filter(
    (col("before.offender_id").isin({127, 128})) | (col("after.offender_id").isin({127, 128}))).show(10)

