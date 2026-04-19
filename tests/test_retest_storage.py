import pyarrow as pa
from stdf_platform.storage import PARTS_SCHEMA, TEST_DATA_SCHEMA


def test_parts_schema_has_retest_num():
    assert PARTS_SCHEMA.get_field_index("retest_num") >= 0
    field = PARTS_SCHEMA.field("retest_num")
    assert pa.types.is_integer(field.type)


def test_test_data_schema_has_retest_num():
    assert TEST_DATA_SCHEMA.get_field_index("retest_num") >= 0
    field = TEST_DATA_SCHEMA.field("retest_num")
    assert pa.types.is_integer(field.type)
