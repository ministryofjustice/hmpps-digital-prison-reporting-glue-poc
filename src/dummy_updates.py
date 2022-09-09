from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import json

import datetime
import random

pos_seed = 3000

global_dict = dict(table='OMS_OWNER.OFFENDERS',
                   op_type='I',
                   op_ts='2013-06-02 22:14:36.000000',
                   current_ts='2015-09-18T13:39:35.447000',
                   pos='00000000000000001444',
                   tokens=[],
                   after=[])

global_dict["tokens"] = dict(R='AADPkvAAEAAEqL2AAA')

print(datetime.datetime.now())

glueContext = GlueContext(SparkContext.getOrCreate())
inputDF = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
    "paths": ["s3://frazers-test-bucket/data/dummy/source/OFFENDERS_202209061845.json"]}, format="json")
inputDF.toDF().show(10)

# json_obj = inputDF.toJSON()
idf = inputDF.toDF()

print(idf.count())
inputs = idf.first()[0]
# jsonrecs = json.loads(str(inputs))

json_list_i = []
json_list_u = []

for rec in inputs:
    # print(rec)
    pos_time_delta = pos_seed + random.randint(1, 10000)
    recdict = rec.asDict()
    new_timestamp = datetime.datetime.strptime(recdict["CREATE_DATE"][:19],
                                               '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(seconds=pos_time_delta)

    recdict["AUDIT_TIMESTAMP"] = str(new_timestamp) + '.500000'
    recdict["CREATE_DATETIME"] = str(new_timestamp) + '.000000'
    recdict["CREATE_DATETIME"] = str(new_timestamp) + '.000000'
    recdict["MODIFIED_DATETIME"] = None
    recdict["MODIFY_DATETIME"] = None
    recdict["BIRTH_DATE"] = recdict["BIRTH_DATE"][:10]
    recdict["CREATE_DATE"] = recdict["CREATE_DATE"][:10]

    global_dict["current_ts"] = str(datetime.datetime.now())
    global_dict["pos"] = "{:020d}".format(pos_time_delta)
    global_dict["op_ts"] = str(new_timestamp) + '.' + str(random.randint(1, 999999))
    global_dict["after"] = recdict
    json_list_i.append(json.dumps(global_dict))
    # updates

    pos_time_delta = pos_time_delta + random.randint(1, 31536000)
    global_dict["before"] = recdict.copy()
    global_dict["current_ts"] = str(datetime.datetime.now())
    global_dict["pos"] = "{:020d}".format(pos_time_delta)
    global_dict["op_type"] = "U"
    # print(global_dict["after"]["CREATE_DATETIME"]+timedelta(seconds=24)
    new_timestamp = new_timestamp + datetime.timedelta(seconds=pos_time_delta)

    global_dict["op_ts"] = str(new_timestamp) + '.' + str(random.randint(1, 999999))
    # global_dict["after"]["AUDIT_TIMESTAMP"] = str(new_timestamp) + '.500000'
    # global_dict["after"]["CREATE_DATETIME"] = str(new_timestamp) + '.000000'
    global_dict["after"]["MODIFIED_DATETIME"] = str(new_timestamp) + '.000000'
    global_dict["after"]["MODIFY_DATETIME"] = str(new_timestamp) + '.000000'
    global_dict["after"]["MODIFY_USER_ID"] = "FRAZERCLAYTON"
    if global_dict["after"]["SEX_CODE"] == 'male':
        global_dict["after"]["TITLE"] = 'Mr'
    if global_dict["after"]["SEX_CODE"] == 'female':
        global_dict["after"]["TITLE"] = 'Mrs'
        if global_dict["after"]["AGE"] < 40:
            global_dict["after"]["TITLE"] = 'Miss'
        if global_dict["after"]["AGE"] > 70:
            global_dict["after"]["TITLE"] = 'Ms'
    # global_dict["after"]["BIRTH_DATE"] = global_dict["after"]["BIRTH_DATE"][:10]
    # global_dict["after"]["CREATE_DATE"] = global_dict["after"]["CREATE_DATE"][:10]
    json_list_u.append(json.dumps(global_dict))
    # print(json_list[0])
dfout_i = spark.read.json(sc.parallelize(json_list_i))
dfout_u = spark.read.json(sc.parallelize(json_list_u))
# dfout = sc.parallelize(json_list).toDF()

dfout_u.show(2, truncate=False)

out_dyf = DynamicFrame.fromDF(dfout_i, glueContext, "out_dyf")
out_dyf_u = DynamicFrame.fromDF(dfout_u, glueContext, "out_dyf_u")
print("INSERTS", out_dyf.count())
print("updates", out_dyf_u.count())

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf,
    connection_type="s3",
    connection_options={"path": "s3://frazers-test-bucket/data/dummy/transac/OFFENDERS/inserts/"},
    format="json")

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf_u,
    connection_type="s3",
    connection_options={"path": "s3://frazers-test-bucket/data/dummy/transac/OFFENDERS/updates/"},
    format="json")
