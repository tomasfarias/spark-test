import copy
from typing import Union, List, Dict

from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, Row, SparkSession


def create_dataframe(
        data: Union[List, Dict], schema: StructType = None, spark: SparkSession = None
) -> Union[List[Row], DataFrame]:
    """
    Create a Spark DataFrame with a list, dict or RDD. If spark is None, will return a
        List[Row] with the rows of the DataFrame (same output as calling DataFrame.collect()).

    :param data: The DataFrame data. If a dict, each key will be a column. If list
        each value will be iterated to return a column (names given by schema).
    :param schema: A schema for the DataFrame. If None, Spark infers it from data.
    :param spark: A SparkSession to create the DataFrame. Returns list of Row if None.
    """

    data = copy.deepcopy(data)
    df_rows: List[Row] = []

    if isinstance(data, dict):

        keys = data.keys()

        while True:
            try:
                df_rows += [Row(**{k: data[k].pop(0) for k in keys})]
            except IndexError:  # raised by pop() when called on an empty list
                break

    elif isinstance(data, list):

        list_data = [[_ for _ in val] for val in data]

        while True:
            try:
                df_rows += [
                    Row(**{f'_{idx + 1}': val.pop(0) for idx, val in enumerate(list_data)})
                ]
            except IndexError:
                break

    else:
        raise TypeError(f'Unsupported type for data: {type(data)}')

    if spark is None:
        return df_rows
    else:
        return spark.createDataFrame(df_rows, schema=schema)
