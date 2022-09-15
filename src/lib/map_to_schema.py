from pyspark.sql.types import DateType, TimestampType, Row
import datetime

"""
functions to map schema to dataframe structure
"""

__author__ = "frazer.clayton@digital.justice.gov.uk"


def get_schema_fields_as_dict(schema):
    """
    return the schema as a dict of fieldname: datatype
    :param schema: schema definition
    :return: schema as dict
    """
    schema_dict = {}
    for fld in schema:
        schema_dict[fld.name] = None
    return schema_dict


def get_schema_field_type(schema, fldname):
    """
    get schema field datatype
    :param schema: schema definition
    :param fldname: field name
    :return: datatype
    """
    for fld in schema:
        if fld.name == fldname:
            return fld.dataType


def format_field(schema, fldname, fld_val):
    """
    format field value as datatype in schema
    :param schema: schema definition
    :param fldname: field name
    :param fld_val: field value
    :return: formatted value
    """

    fldtype = get_schema_field_type(schema=schema, fldname=fldname)
    new_val = fld_val
    if fld_val is not None:
        if fldtype == DateType():
            # print("datetype", fldname, fld_val)
            new_val = datetime.datetime.strptime(fld_val, "%Y-%m-%d")
        if fldtype == TimestampType():
            # print("timestamptype", fldname, fld_val)
            new_val = datetime.datetime.strptime(fld_val[:26], "%Y-%m-%d %H:%M:%S.%f")
    return new_val


def mapper(row_in, schema):
    """
    map rows to schema
    :param row_in: row containing unmapped record
    :param schema: schema definition
    :return: row type in correct schema format
    """

    # print("row_in")
    # print(row_in["after"])
    # new_row = Row()
    # print("#####")

    new_row_dict = get_schema_fields_as_dict(schema)

    if row_in["op_type"] != "D":
        row_out = row_in["after"]
    else:
        row_out = row_in["before"]

    row_dict = row_out.asDict()

    for fld_name in row_dict:
        if fld_name.lower() == 'modified_datetime':
            new_row_dict['modify_datetime'] = row_dict[fld_name]
        else:
            new_row_dict[fld_name.lower()] = format_field(schema=schema, fldname=fld_name.lower(),
                                                          fld_val=row_dict[fld_name])
    new_row_dict["admin_hash"] = row_in["after_hash"]

    new_row_dict["previous_hash"] = row_in["before_hash"]
    new_row_dict["admin_gg_pos"] = row_in["pos"]
    new_row_dict["admin_gg_op_ts"] = format_field(schema=schema, fldname='admin_gg_op_ts',
                                                  fld_val=row_in["op_ts"])
    new_row_dict["admin_event_ts"] = datetime.datetime.now()
    new_row_dict["event_type"] = row_in["op_type"]
    # print(new_row_dict)
    return Row(**new_row_dict)
