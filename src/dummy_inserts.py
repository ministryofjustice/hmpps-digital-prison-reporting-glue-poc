from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import json

import datetime
import random

pos_seed = 5000

global_dict = dict(table='OMS_OWNER.OFFENDERS',
                   op_type='I',
                   op_ts='2013-06-02 22:14:36.000000',
                   current_ts='2015-09-18T13:39:35.447000',
                   pos='00000000000000001444',
                   tokens={},
                   after=[],
                   before=[])

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

json_list = []

for rec in inputs:
    pos_seed = pos_seed + random.randint(1, 999999)
    global_dict["after"] = rec.asDict()
    global_dict["before"] = rec.asDict()
    global_dict["current_ts"] = str(datetime.datetime.now())
    global_dict["pos"] = "{:020d}".format(pos_seed)
    # print(global_dict["after"]["CREATE_DATETIME"]+timedelta(seconds=24)
    new_timestamp = datetime.datetime.strptime(global_dict["after"]["CREATE_DATETIME"][:19],
                                               '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(seconds=pos_seed)

    global_dict["op_ts"] = str(new_timestamp) + '.' + \
        str(random.randint(1, 999999))
    global_dict["after"]["AUDIT_TIMESTAMP"] = str(new_timestamp) + '.500000'
    global_dict["after"]["CREATE_DATETIME"] = str(new_timestamp) + '.000000'
    global_dict["after"]["MODIFIED_DATETIME"] = str(new_timestamp) + '.000000'
    global_dict["after"]["MODIFY_DATETIME"] = str(new_timestamp) + '.000000'
    global_dict["after"]["BIRTH_DATE"] = global_dict["after"]["BIRTH_DATE"][:10]
    global_dict["after"]["CREATE_DATE"] = global_dict["after"]["CREATE_DATE"][:10]
    json_list.append(json.dumps(global_dict))

    # print(json_list[0])
dfout = spark.read.json(sc.parallelize(json_list))
# dfout = sc.parallelize(json_list).toDF()

dfout.show(2, truncate=False)

print(dfout.count())

out_dyf = DynamicFrame.fromDF(dfout, glueContext, "out_dyf")

print(out_dyf.count())

glueContext.write_dynamic_frame.from_options(
    frame=out_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://frazers-test-bucket/data/dummy/transac/OFFENDERS"},
    format="json")
