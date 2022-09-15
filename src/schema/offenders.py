from pyspark.sql.types import (
    StringType,
    StructType,
    LongType,
    DoubleType,
    FloatType,
    IntegerType,
    BooleanType,
    BinaryType,
    ArrayType,
    MapType,
    DateType,
    TimestampType,
    StructField,
)

possible_types = {
    1: lambda t: DoubleType(),
    2: lambda t: FloatType(),
    3: lambda t: LongType(),
    4: lambda t: LongType(),
    5: lambda t: IntegerType(),
    8: lambda t: BooleanType(),
    "varchar2": lambda t: StringType(),
    # 11: lambda t: schema_for_spark(t.message_type),
    12: lambda t: BinaryType(),
    "number": lambda t: IntegerType(),
    14: lambda t: StringType(),  # enum type
    15: lambda t: IntegerType(),
    16: lambda t: LongType(),
    17: lambda t: IntegerType(),
    18: lambda t: LongType(),
    "date": lambda t: DateType(),
    "timestamp": lambda t: TimestampType(),
}


def __type_for(datatype):
    """
    return types for field
    """
    get_type = possible_types.get(datatype, lambda t: StringType())

    return get_type(datatype)


ddl = (
    "OFFENDER_ID number,OFFENDER_NAME_SEQ number,ID_SOURCE_CODE varchar2(48),LAST_NAME varchar2(140),NAME_TYPE "
    "varchar2(48),FIRST_NAME varchar2(140),MIDDLE_NAME varchar2(140),BIRTH_DATE date,SEX_CODE varchar2(48),"
    "SUFFIX varchar2(48),LAST_NAME_SOUNDEX varchar2(24),BIRTH_PLACE varchar2(100),BIRTH_COUNTRY_CODE varchar2(48),"
    "CREATE_DATE date,LAST_NAME_KEY varchar2(140),ALIAS_OFFENDER_ID number,FIRST_NAME_KEY varchar2(140),"
    "MIDDLE_NAME_KEY varchar2(140),OFFENDER_ID_DISPLAY varchar2(40),ROOT_OFFENDER_ID number,CASELOAD_TYPE varchar2("
    "48),MODIFY_USER_ID varchar2(128),MODIFY_DATETIME timestamp(9),ALIAS_NAME_TYPE varchar2(48),PARENT_OFFENDER_ID "
    "number,UNIQUE_OBLIGATION_FLAG varchar2(4),SUSPENDED_FLAG varchar2(4),SUSPENDED_DATE date,RACE_CODE varchar2("
    "48),REMARK_CODE varchar2(48),ADD_INFO_CODE varchar2(48),BIRTH_COUNTY varchar2(80),BIRTH_STATE varchar2(80),"
    "MIDDLE_NAME_2 varchar2(140),TITLE varchar2(48),AGE number,CREATE_USER_ID varchar2(160),LAST_NAME_ALPHA_KEY "
    "varchar2(4),CREATE_DATETIME timestamp(9),NAME_SEQUENCE varchar2(48),AUDIT_TIMESTAMP timestamp(9),AUDIT_USER_ID "
    "varchar2(128),AUDIT_MODULE_NAME varchar2(260),AUDIT_CLIENT_USER_ID varchar2(256),AUDIT_CLIENT_IP_ADDRESS "
    "varchar2(156),AUDIT_CLIENT_WORKSTATION_NAME varchar2(256),AUDIT_ADDITIONAL_INFO varchar2(1024)"
)


def get_schema(with_event_type=False):
    field_list = ddl.split(",")
    struct_list = []

    for field in field_list:
        field_name, field_type = field.split(" ")
        field_name = field_name.lower()
        field_type = field_type.split("(")[0].lower()

        struct_list.append(StructField(field_name, __type_for(field_type), True))

    struct_list.append(StructField("admin_hash", StringType(), True))
    struct_list.append(StructField("admin_gg_pos", StringType(), True))
    struct_list.append(StructField("admin_gg_op_ts", TimestampType(), True))
    struct_list.append(StructField("admin_event_ts", TimestampType(), True))
    if with_event_type:
        struct_list.append(StructField("event_type", StringType(), True))
    return StructType(struct_list)


def get_primary_key():
    return "offender_id"


if __name__ == "__main__":
    print(get_schema())
    schema = get_schema()
    count = 0
    for fld in schema:
        count = count + 1
        print(fld.name, fld.dataType)
    print(count)
