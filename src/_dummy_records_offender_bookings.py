from awsglue.glue_shell import sc
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import json

import datetime
import random

from pyspark.shell import spark

config_dict = dict(
    source_bucket="dpr-demo-development-20220916083016121000000001",
    target_bucket="dpr-demo-development-20220916083016121000000001",
    source="data/dummy/source/OFFENDER_BOOKINGS_202209201358.json",
    target_json="data/dummy/kinesis/transac/json/",
    target_parquet="data/dummy/kinesis/transac/parquet/",
    schema="oms_owner",
    table="offender_bookings",
)

pos_seed = 1000000
rec_count = 0
global_dict = dict(
    table="OMS_OWNER.OFFENDER_BOOKINGS",
    op_type="I",
    op_ts="2013-06-02 22:14:36.000000",
    current_ts="2015-09-18T13:39:35.447000",
    pos="00000000000010001444",
    tokens={},
    after={},
    before={},
)

input_path = config_dict["source_bucket"] + "/" + config_dict["source"]
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
inputDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://{}".format(input_path)]},
    format="json",
)
inputDF.toDF().show(10)

# json_obj = inputDF.toJSON()
idf = inputDF.toDF()

print(idf.count())
inputs = idf.first()[0]
# jsonrecs = json.loads(str(inputs))

json_list_b = []
json_list_i = []
json_list_u = []
json_list_d = []

for rec in inputs:
    rec_count = rec_count + 1
    pos_time_delta = pos_seed + random.randint(1, 10000)
    recdict = rec.asDict()
    new_timestamp = datetime.datetime.strptime(recdict["CREATE_DATETIME"][:19], "%Y-%m-%dT%H:%M:%S") + datetime.timedelta(
        seconds=pos_time_delta
    )
    new_timestamp = datetime.datetime.now() - datetime.timedelta(seconds=pos_time_delta)

    recdict["AUDIT_TIMESTAMP"] = str(new_timestamp) + ".500000"
    recdict["CREATE_DATETIME"] = str(new_timestamp) + ".000000"
    recdict["CREATE_DATETIME"] = str(new_timestamp) + ".000000"
    # recdict["MODIFIED_DATETIME"] = None
    recdict["MODIFY_DATETIME"] = None
    recdict["BOOKING_BEGIN_DATE"] = recdict["BOOKING_BEGIN_DATE"][:10]
    recdict["BOOKING_CREATED_DATE"] = str(new_timestamp)[:10]
    # recdict["CREATE_DATE"] = recdict["CREATE_DATE"][:10]
    # inserts
    global_dict["op_type"] = "I"
    global_dict["tokens"] = {}
    global_dict["tokens"] = dict(R="AADPkvAAEAAEqL2AAA")
    global_dict["current_ts"] = str(datetime.datetime.now())
    global_dict["pos"] = "{:020d}".format(pos_time_delta)
    global_dict["op_ts"] = str(new_timestamp) + "." + str(random.randint(1, 10))
    global_dict["before"] = {}
    global_dict["after"] = {}
    global_dict.pop("before")
    global_dict.pop("after")
    global_dict["after"] = recdict
    if rec_count < 20:
        json_list_b.append(json.dumps(global_dict))
    else:
        json_list_i.append(json.dumps(global_dict))
    # updates

    if rec_count > 15:

        pos_time_delta = pos_time_delta + random.randint(1, 100000)
        global_dict["before"] = recdict.copy()
        global_dict["current_ts"] = str(datetime.datetime.now())
        global_dict["pos"] = "{:020d}".format(pos_time_delta)
        global_dict["op_type"] = "U"
        global_dict["tokens"] = {}
        global_dict["tokens"] = dict(R="AADPkvAAEAAEqLzAAA")
        # print(global_dict["after"]["CREATE_DATETIME"]+timedelta(seconds=24)
        new_timestamp = new_timestamp + datetime.timedelta(seconds=pos_time_delta)

        global_dict["op_ts"] = str(new_timestamp) + "." + str(random.randint(1, 10))
        # global_dict["after"]["AUDIT_TIMESTAMP"] = str(new_timestamp) + '.500000'
        # global_dict["after"]["CREATE_DATETIME"] = str(new_timestamp) + '.000000'
        # global_dict["after"]["MODIFIED_DATETIME"] = str(new_timestamp) + ".000000"
        global_dict["after"]["MODIFY_DATETIME"] = str(new_timestamp) + ".000000"
        global_dict["after"]["MODIFY_USER_ID"] = "FRAZERCLAYTON"
        # if global_dict["after"]["SEX_CODE"] == "male":
        #     global_dict["after"]["TITLE"] = "Mr"
        # if global_dict["after"]["SEX_CODE"] == "female":
        #     global_dict["after"]["TITLE"] = "Mrs"
        #     if global_dict["after"]["AGE"] < 40:
        #         global_dict["after"]["TITLE"] = "Miss"
        #     if global_dict["after"]["AGE"] > 70:
        #         global_dict["after"]["TITLE"] = "Ms"
        global_dict["after"]["IN_OUT_STATUS"] = "OUT"
        global_dict["after"]["BOOKING_END_DATE"] = global_dict["after"]["MODIFY_DATETIME"][:10]
        # global_dict["after"]["BIRTH_DATE"] = global_dict["after"]["BIRTH_DATE"][:10]
        # global_dict["after"]["CREATE_DATE"] = global_dict["after"]["CREATE_DATE"][:10]
        json_list_u.append(json.dumps(global_dict))

    # deletes
    if rec_count > 10 and rec_count < 20:
        pos_time_delta = pos_time_delta + random.randint(1, 100000)
        new_timestamp = new_timestamp + datetime.timedelta(seconds=pos_time_delta)
        recdict_before = recdict.copy()
        recdict_after = global_dict["after"].copy()
        global_dict["before"] = {}
        global_dict.pop("before")
        global_dict.pop("after")

        # make alternate delete invalid
        if rec_count % 2 == 0:
            global_dict["before"] = recdict_after
        else:
            global_dict["before"] = recdict_before
        global_dict["current_ts"] = str(datetime.datetime.now())
        global_dict["op_ts"] = str(new_timestamp) + "." + str(random.randint(1, 10))
        global_dict["pos"] = "{:020d}".format(pos_time_delta)
        global_dict["op_type"] = "D"
        global_dict["tokens"] = {}
        global_dict["tokens"] = dict(
            R="AADPkvAAEAAEqLzAAC",
            L="206080450",
        )
        global_dict["tokens"]["6"] = "9.0.80330"

        json_list_d.append(json.dumps(global_dict))

dfout_i = spark.read.json(sc.parallelize(json_list_i))
dfout_u = spark.read.json(sc.parallelize(json_list_u))
dfout_d = spark.read.json(sc.parallelize(json_list_d))
dfout_b = spark.read.json(sc.parallelize(json_list_b))
# dfout = sc.parallelize(json_list).toDF()

# dfout_u.show(2, truncate=False)

out_dyf = DynamicFrame.fromDF(dfout_i, glueContext, "out_dyf")
out_dyf_u = DynamicFrame.fromDF(dfout_u, glueContext, "out_dyf_u")
out_dyf_d = DynamicFrame.fromDF(dfout_d, glueContext, "out_dyf_d")
out_dyf_b = DynamicFrame.fromDF(dfout_b, glueContext, "out_dyf_b")

print("records", rec_count)
print("BASE", out_dyf_b.count())
print("INSERTS", out_dyf.count())
print("updates", out_dyf_u.count())
print("deletes", out_dyf_d.count())

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf_b,
    connection_type="s3",
    connection_options={"path": "s3://{}/base/".format(output_path)},
    format="json",
)

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf,
    connection_type="s3",
    connection_options={"path": "s3://{}/inserts/".format(output_path)},
    format="json",
)

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf_u,
    connection_type="s3",
    connection_options={"path": "s3://{}/updates/".format(output_path)},
    format="json",
)

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf_d,
    connection_type="s3",
    connection_options={"path": "s3://{}/deletes/".format(output_path)},
    format="json",
)
