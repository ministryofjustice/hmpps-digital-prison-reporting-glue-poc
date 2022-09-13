from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import json

import datetime
import random

pos_seed = 3000


glueContext = GlueContext(SparkContext.getOrCreate())
inputDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={
        "paths": [
            "s3://dpr-demo-development-20220906101710889000000001/dummy/kinesis/transac/json/OMS_OWNER/OFFENDERS/"
        ]
    },
    format="json",
)
local_df = inputDF.toDF()
# unested_df = inputDF.unnest().toDF()

local_df.select(
    col("table"),
    col("op_type"),
    col("op_ts"),
    col("current_ts"),
    col("pos"),
    col("after.offender_id"),
    col("after.first_name"),
    col("after.last_name"),
).show()

local_df.printSchema()
# unested_df.printSchema()
glueContext.write_dynamic_frame.from_options(
    frame=inputDF,
    connection_type="s3",
    connection_options={
        "path": "s3://dpr-demo-development-20220906101710889000000001/dummy/kinesis/transac/parquet/OMS_OWNER/OFFENDERS/"
    },
    format="parquet",
)
