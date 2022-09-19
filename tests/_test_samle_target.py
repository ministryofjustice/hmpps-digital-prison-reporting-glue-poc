from src._sample_target import mapper, show_table
from src.merge_to_parquet import config_dict, add_hash_drop_tokens
from src.schema.offenders import get_schema


def test_run(spark_session):
    config_dict["read_path"] = (
            "./tests/" + config_dict["target_json"] + "/" + config_dict["schema"] + "/" + config_dict["table"]
    )
    config_dict["write_path"] = (
            "./tests/data/dummy/database/" + config_dict["schema"] + "/" + config_dict["table"]
    )
    target_schema = get_schema()
    #print(target_schema)
    df = spark_session.read.json(config_dict["read_path"] + "/base/")
    df = add_hash_drop_tokens(frame=df, hash_fields=["after"])

    temp_rdd = df.rdd.map(lambda row: mapper(row_in=row, schema=target_schema))

    local_df_out = temp_rdd.toDF(schema=target_schema)

    show_table(local_df_out)

    local_df_out.write.format("parquet").mode("overwrite").save(config_dict["write_path"])