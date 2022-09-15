# configuration
config_gg_events = dict(
    source_bucket="dpr-demo-development-20220906101710889000000001",
    source="data/dummy/kinesis/transac/parquet",
    schema="oms_owner",
    table="offenders",
)
config_target_table = dict(
    target_bucket="dpr-demo-development-20220906101710889000000001",
    target_final="data/dummy/database/",
    schema="oms_owner",
    table="offenders",
    # partition_by = ["date", "time"]
    partition_by=["create_date"],
)


def update_config():
    """
    Update configuration with elements describing paths to data
    :return: None
    """

    config_gg_events["path"] = (
        config_gg_events["source_bucket"]
        + "/"
        + config_gg_events["source"]
        + "/"
        + config_gg_events["schema"]
        + "/"
        + config_gg_events["table"]
    )

    config_target_table["path"] = (
        config_target_table["target_bucket"]
        + "/"
        + config_target_table["target_final"]
        + "/"
        + config_target_table["schema"]
        + "/"
        + config_target_table["table"]
    )
