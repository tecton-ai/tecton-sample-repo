from pathlib import Path
from pyspark.context import SparkContext
import pytest

@pytest.fixture(scope="session")
def spark_session():
    import findspark
    spark_path = Path('.tecton/spark')
    findspark.init(spark_home=str(spark_path.absolute()))

    from pyspark.sql import SparkSession    
    spark = SparkSession.builder.master("local").appName('pytest_spark_session').getOrCreate()

    from pyspark.context import SparkContext
    SparkContext._active_spark_context = spark.sparkContext

    from tecton.tecton_context import TectonContext
    TectonContext._current_context_instance = TectonContext(spark)

    yield spark

    spark.stop()
