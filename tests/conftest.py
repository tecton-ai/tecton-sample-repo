import pytest
import tempfile
from importlib import resources

from pyspark.sql import SparkSession
import tecton

@pytest.fixture(scope='module')
def my_custom_spark_session():
    """Returns a custom spark session configured for use in Tecton unit testing."""
    with resources.path("tecton_spark.jars", "tecton-udfs-spark-3.jar") as path:
        tecton_udf_jar_path = str(path)

    with tempfile.TemporaryDirectory() as tempdir:
	    spark = (
	        SparkSession.builder.appName("my_custom_spark_session")
	        .config("spark.jars", tecton_udf_jar_path)
	        # This short-circuit's Spark's attempt to auto-detect a hostname for the master address, which can lead to
	        # errors on hosts with "unusual" hostnames that Spark believes are invalid.
	        .config("spark.driver.host", "localhost")
	        .config("spark.sql.warehouse.dir", tempdir)
	        .config("spark.sql.session.timeZone", "UTC")
	        .getOrCreate()
	    )
	    try:
	        tecton.set_tecton_spark_session(spark)
	        yield spark
	    finally:
	        spark.stop()
