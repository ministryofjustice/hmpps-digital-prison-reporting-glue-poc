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

write_path = (
    config_dict["target_bucket"]
    + "/"
    + config_dict["target_parquet"]
    + "/"
    + config_dict["schema"]
    + "/"
    + config_dict["table"]
)

glueContext = GlueContext(SparkContext.getOrCreate())
inputDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", connection_options={"paths": ["s3://{}".format(write_path)]}, format="parquet"
)
local_df = inputDF.toDF()


local_df.select(
    col("table"),
    col("op_type"),
    col("op_ts"),
    col("current_ts"),
    col("pos"),
    col("after.offender_id"),
    col("after.first_name"),
    col("after.last_name"),
    col("after.title"),
).show()

print("records", local_df.count())
