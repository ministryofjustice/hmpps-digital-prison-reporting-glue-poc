
def rename_columns(frame):
    new_column_name_list = list(map(lambda x: "__{}".format(x), frame.columns))
    return frame.toDF(*new_column_name_list)
