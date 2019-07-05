import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import ShortType

from spark_test.helpers import create_dataframe
from spark_test.testing import assert_rdd_equal
from spark_test.testing import assert_dataframe_equal
from spark_test.testing import assert_schema_equal
from spark_test.testing import assert_row_equal


def test_assert_schema_equal(spark):
    expected = StructType(
        [
            StructField('Name', StringType(), True),
            StructField('Age', ShortType(), True)
        ]
    )
    result = StructType(
        [
            StructField('Name', StringType(), True),
            StructField('Age', ShortType(), True)
        ]
    )

    assert_schema_equal(expected, result)

    result = StructType(
        [
            StructField('Name', StringType(), True),
            StructField('Age', IntegerType(), True)
        ]
    )

    with pytest.raises(AssertionError):
        assert_schema_equal(expected, result)

    result = StructType(
        [
            StructField('Name', StringType(), True),
            StructField('age', ShortType(), True)
        ]
    )

    with pytest.raises(AssertionError):
        assert_schema_equal(expected, result)

    result = StructType(
        [
            StructField('Name', StringType(), True),
            StructField('Age', ShortType(), False)
        ]
    )

    with pytest.raises(AssertionError):
        assert_schema_equal(expected, result)


def test_assert_rdd_equal(spark):
    sc = spark.sparkContext

    rd1 = sc.parallelize([1, 2, 3])
    rd2 = sc.parallelize([1, 2, 3])

    assert_rdd_equal(rd1, rd2)

    l1 = [1, 2, 3]

    assert_rdd_equal(l1, rd2)

    rd2 = sc.parallelize([1, 2, 4])

    with pytest.raises(AssertionError):
        assert_rdd_equal(rd1, rd2)

    rd1 = sc.parallelize([1, 2, 3])
    rd2 = sc.parallelize([1, 2])

    with pytest.raises(AssertionError):
        assert_rdd_equal(rd1, rd2)


def test_assert_frame_equal(spark):

    expected = create_dataframe({'Name': ['Tom', 'Charlie'], 'Age': [25, 24]}, spark=spark)
    result = create_dataframe({'Name': ['Tom', 'Charlie'], 'Age': [25, 24]}, spark=spark)

    assert_dataframe_equal(expected, result)

    result = create_dataframe({'Name': ['Tom', 'Charlie'], 'Age': [25, 23]}, spark=spark)

    with pytest.raises(AssertionError):
        assert_dataframe_equal(expected, result)

    result = create_dataframe({'Name': ['Tom'], 'Age': [25]}, spark=spark)

    with pytest.raises(AssertionError):
        assert_dataframe_equal(expected, result)

    new_schema = StructType(
        [
            StructField('Age', ShortType(), True),
            StructField('Name', StringType(), True)
        ]
    )
    result = create_dataframe(
        {'Name': ['Tom', 'Charlie'], 'Age': [25, 24]}, schema=new_schema, spark=spark
    )

    with pytest.raises(AssertionError):
        assert_dataframe_equal(expected, result)

    assert_dataframe_equal(expected, result, check_schema=False)

    result = create_dataframe({'Name': ['Charlie', 'Tom'], 'Age': [24, 25]}, spark=spark)

    with pytest.raises(AssertionError):
        assert_dataframe_equal(expected, result)

    assert_dataframe_equal(expected, result, check_order=False)


def test_assert_row_equal():

    left = Row(Name='Tom', Pet='Cat', Age=25, Status=1.0)
    right = Row(Name='Tom', Pet='Cat', Age=25, Status=1.0)

    assert_row_equal(left, right)

    right = Row(Name='Tom', Pet='Cat', Age=24, Status=1.0)

    with pytest.raises(AssertionError):
        assert_row_equal(left, right)

    right = Row(Name='Tom', Age=25, Status=1.0)

    with pytest.raises(AssertionError):
        assert_row_equal(left, right)
