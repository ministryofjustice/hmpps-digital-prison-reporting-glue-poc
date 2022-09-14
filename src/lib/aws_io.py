"""
functions to access s3 based object via glue catalog or s3
"""


__author__ = "frazer.clayton@digital.justice.gov.uk"


FORMAT = "parquet"


def write_catalog_df(gluecontext, schema, table, path, frame, partition_by_list):
    """
    write output using glue catalog
    :param table: table name
    :param schema: database name
    :param partition_by_list: partition fields as list
    :param gluecontext: Glue context
    :param path: data path
    :param frame: Glue Dynamic Frame
    :return:None
    """
    additionaloptions = {"enableUpdateCatalog": True, "partitionKeys": partition_by_list}

    gluecontext.write_dynamic_frame_from_catalog(
        frame=frame,
        database=schema,
        table_name=table,
        connection_type="s3",
        connection_options={"path": "s3://{}/".format(path)},
        additional_options=additionaloptions,
        format=FORMAT,
    )


def write_s3_df(gluecontext, path, frame, partition_by_list):
    """
    write output to S3
    :param partition_by_list: partition fields as list
    :param gluecontext: Glue context
    :param path: data path
    :param frame: Glue Dynamic Frame
    :return:None
    """
    gluecontext.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={
            "path": "s3://{}/".format(path),
            "partitionKeys": partition_by_list,
        },
        format=FORMAT,
    )


def read_s3_to_df(gluecontext, path, key_suffix=None, file_format=FORMAT):
    """
    Read from S3 into Dataframe
    :param file_format: file format
    :param gluecontext: Glue context
    :param path: datapath
    :param key_suffix: suffix to be added to path
    :return: spark dataframe
    """
    read_path = "s3://{}/".format(path)
    if key_suffix is not None:
        read_path = read_path + "{}/".format(key_suffix)
    input_dydf = gluecontext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [read_path]},
        format=file_format,
    )
    return input_dydf.toDF()
