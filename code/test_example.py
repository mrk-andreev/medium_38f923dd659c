import pytest
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.getOrCreate()


def evaluate_feature_avg_price_per_merchant(df: DataFrame) -> DataFrame:
    return df.groupBy('merchant').agg(avg('price').alias('avg_price'))


def test_evaluate_feature_avg_price_per_merchant(spark: SparkSession):
    df = spark.createDataFrame([
        Row(merchant=1, price=1.0),
        Row(merchant=1, price=2.0),
        Row(merchant=2, price=3.0),
    ]).orderBy('merchant')
    df_expected = spark.createDataFrame([
        Row(merchant=1, price=1.5),
        Row(merchant=2, price=3.0),
    ]).orderBy('merchant')

    df_out = evaluate_feature_avg_price_per_merchant(df)

    assert df_out.collect() == df_expected.collect()
