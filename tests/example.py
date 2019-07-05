import pytest

from spark_test.testing import assert_dataframe_equal


def transform(df):
    """Fill nulls with 0, sum 10 to Age column and only return distinct rows"""

    df = df.na.fill(0)
    df = df.withColumn('Age', df['Age'] + 10)
    df = df.distinct()

    return df


def bugged_transform(df):
    """A bugged version of transform to make tests fail"""

    df = df.na.fill(1)  # Whoops! Should be 0!
    df = df.withColumn('Age', df['Age'] + 10)
    df = df.distinct()

    return df


@pytest.mark.parametrize('transform', (transform, bugged_transform))
def test_transform(spark, transform):

    input_df = spark.createDataFrame(
      [['Charlie', 24], ['Dan', None], ['Tom', 25], ['Tom', 25]],
      schema=['Name', 'Age']
    )

    expected = spark.createDataFrame(
      [['Charlie', 34], ['Dan', 10], ['Tom', 35]],
      schema=['Name', 'Age']
    )
    result = transform(input_df)

    assert_dataframe_equal(expected, result)
