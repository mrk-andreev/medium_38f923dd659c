# [How to write tests for PySpark](https://medium.com/@mark.andreev/how-to-write-tests-for-pyspark-38f923dd659c)

Test spark function like `evaluate_feature_avg_price_per_merchant`

```python
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg


def evaluate_feature_avg_price_per_merchant(df: DataFrame) -> DataFrame:
    return df.groupBy('merchant').agg(avg('price').alias('avg_price'))
```

Using `pytest.fixture`

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.getOrCreate()
```

## Requirements

Env:
```
JAVA_HOME=~/.jdks/corretto-1.8.0_362
```

Python deps:
```
pytest==7.2.1
pyspark==3.3.1
pandas==1.5.3
```
