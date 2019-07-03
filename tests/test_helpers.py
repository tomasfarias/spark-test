import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType

from spark_test.helpers import create_dataframe
from spark_test.testing import assert_dataframe_equal


def test_create_dataframe(spark):
    schema = StructType([
        StructField('name', StringType(), True),
        StructField('age', IntegerType(), False)
    ])

    expected = spark.createDataFrame(
        [Row(name='Tom', age=25), Row(name='Charlie', age=24)], schema=schema
    )
    result = create_dataframe(
        {'name': ['Tom', 'Charlie'], 'age': [25, 24]}, schema=schema, spark=spark
    )

    assert_dataframe_equal(expected, result)

    expected = spark.createDataFrame(
        [Row(name='Tom', age=25), Row(name='Charlie', age=24)]
    )
    result = create_dataframe(
        {'name': ['Tom', 'Charlie'], 'age': [25, 24]}, spark=spark
    )

    assert_dataframe_equal(expected, result)

    with pytest.raises(TypeError):
        result = create_dataframe(1)
