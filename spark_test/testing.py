from collections import Counter
from typing import Mapping, Collection, Union

import pyspark
from pyspark.rdd import RDD
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession, Row

from spark_test.helpers import create_dataframe


def assert_rdd_equal(expected: Collection, result: RDD, check_order: bool = True):
    """
    Compare two RDD or one RDD with a Collection

    :param expected: A Collection to compare. For convenience, doesn't need to be a RDD.
    :param result: The RDD to compare.
    :param check_order: Compare the order of values.
    """

    if isinstance(expected, RDD):
        expected = expected.collect()
    else:
        expected = [_ for _ in expected]

    result = result.collect()

    # length comparison
    msg = f'RDD length {len(result)} does not match expected {len(expected)}'
    assert len(expected) == len(result), msg

    # value comparison
    if check_order is True:
        assert expected == result
    else:
        assert Counter(expected) == Counter(result)


def assert_dataframe_equal(
        expected: Union[Mapping, DataFrame], result: DataFrame, check_order: bool = True,
        check_schema: bool = True, schema: StructType = None, spark: SparkSession = None
):
    """
    Compare two DataFrames or one DataFrame with a mapping

    :param expected: A mapping to compare. For convenience, doesn't need to be a DataFrame.
    :param result: The DataFrame to compare.
    :param check_order: Compare the order of rows.
    :param check_schema: Compare schemas.
    :param schema: Schema to compare. Used if expected is not a DataFrame.
    """

    if isinstance(expected, DataFrame):
        expected_df = expected
    elif isinstance(expected, dict):
        expected_df = create_dataframe(expected, schema=schema, spark=spark)
    else:
        raise(TypeError(f'{type(expected)} is not a valid type.'))

    expected_rows = expected_df.collect()
    result_rows = result.collect()

    # length comparison
    msg = (
        'Different length\n'
        f' +  where left={len(result_rows)}\n'
        f' +  where right={len(expected_rows)}'
    )
    assert len(expected_rows) is len(result_rows), msg

    # schema comparison
    if check_schema is True:
        if isinstance(expected, pyspark.sql.DataFrame):
            expected_schema = expected.schema
        elif schema is not None:
            expected_schema = schema
        else:
            raise ValueError(
                'Must pass a SparkSession if comparing schema without a DataFrame.'
            )

        assert_schema_equal(expected_schema, result.schema)

    # row comparison
    if check_order is True:
        for idx, (left_row, right_row) in enumerate(zip(expected_rows, result_rows)):

            try:
                assert_row_equal(left_row, right_row)
            except AssertionError as e:
                e.args = (f'On row {idx}.' + e.args[0], )
                raise

    else:
        expected_count = Counter(expected_rows)
        result_count = Counter(result_rows)

        left_keys_sorted = sorted(expected_count.keys())
        right_keys_sorted = sorted(result_count.keys())

        for idx, (left_row, right_row) in enumerate(zip(left_keys_sorted, right_keys_sorted)):

            try:
                assert_row_equal(left_row, right_row)
            except AssertionError as e:
                e.args = (f'On row {idx}.' + e.args[0], )
                raise

            msg = (
                '{left_row} appears a different amount of times:\n'
                ' +  where left appears {left_count} times\n'
                ' +  where Right: appears {right_count} times'
            )
            assert expected_count[left_row] == result_count[right_row], msg.format(
                left_row=left_row, left_count=expected_count[left_row],
                right_count=result_count[right_row]
            )


def assert_row_equal(left: Row, right: Row, check_field_order: bool = True):
    """
    Comparte two lists of pyspark.sql.Row

    :param left: A Row to compare.
    :param right: Another Row to compare.
    :check_order: Compare the order of rows or ignore it.
    """

    left_d = left.asDict()
    right_d = right.asDict()

    # fields comparison
    if not left_d.keys() == right_d.keys():
        # Something's not right, check which set is different
        extra_l = left_d.keys() - right_d.keys()
        extra_r = right_d.keys() - left_d.keys()

        if extra_l is not set() and extra_r is not set():
            msg = (
                'Both rows contain extra elements\n'
                ' +  where left={l_fields}\n'
                ' +  where right={r_fields}'
            )
            raise(AssertionError(msg.format(l_fields=extra_l, r_fields=extra_r)))

        elif extra_l is not set() and extra_r is set():
            msg = (
                'Left row contains extra elements: {l_fields}'
            )
            raise(AssertionError(msg.format(l_fields=extra_l)))

        else:
            msg = (
                'Right row contains extra elements: {r_fields}'
            )
            raise(AssertionError(msg.format(r_fields=extra_r)))

    # values comparison
    msg = (
        'Values for {field} do not match\n'
        ' +  where left={l_value}\n'
        ' +  where right={r_value}'
    )

    for key in left_d.keys():

        assert left_d[key] == right_d[key], msg.format(
            field=key, l_value=left_d[key], r_value=right_d[key]
        )


def assert_schema_equal(left: StructType, right: StructType):
    """
    Assert left schema is equal to right schema by comparing lenght, fields and
    needConversion

    :param left: A schema to compare.
    :param right: The other schema to compare.
    """

    # length comparison
    msg = f'Left length {len(left)} does not match right length {len(right)}'
    assert len(left) is len(right), msg

    # fields comparison
    msg = (
        'Difference in schema when comparing fields\n'
        " +  where {l_name}'s {attr} = {l_val}\n"
        " +  where {r_name}'s {attr} = {r_val}"
    )

    for l_field, r_field in zip(left, right):

        assert l_field.name == r_field.name, msg.format(
            attr='name', l_name=l_field.name, l_val=l_field.name,
            r_val=r_field.name, r_name=r_field.name
        )

        assert l_field.dataType == r_field.dataType, msg.format(
            attr='dataType', l_name=l_field.name, l_val=l_field.dataType,
            r_val=r_field.dataType, r_name=r_field.name
        )

        assert l_field.nullable is r_field.nullable, msg.format(
            attr='nullable', l_name=l_field.name, l_val=l_field.nullable,
            r_val=r_field.nullable, r_name=r_field.name
        )

        assert l_field.metadata == r_field.metadata, msg.format(
            attr='metadata', l_name=l_field.name, l_val=l_field.metadata,
            r_val=r_field.metadata, r_name=r_field.name
        )

        assert l_field.needConversion() == r_field.needConversion(), msg.format(
            attr='needConversion', l_name=l_field.name, l_val=l_field.needConversion(),
            r_val=r_field.needConversion(), r_name=r_field.name
        )
