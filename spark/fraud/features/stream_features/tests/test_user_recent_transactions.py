import os
from datetime import datetime, timedelta, timezone

import pandas
import pytest
import pytz
import tecton

from fraud.features.stream_features.user_recent_transactions import user_recent_transactions
from importlib import resources
from pyspark.sql import SparkSession


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a Tecton-defined PySpark session for testing
# Spark transformations and feature views. This session can be configured as needed by the user. If an entirely new
# session is needed, then you can create your own and set it with `tecton.set_tecton_spark_session()`.
@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_recent_transactions(my_custom_spark_session):
    data = [
        ("user_1", datetime(2022, 5, 1), 100, "2022", "05", "01"),
        ("user_1", datetime(2022, 5, 1), 200, "2022", "05", "01"),
        ("user_1", datetime(2022, 5, 1), 300, "2022", "05", "01"),
        ("user_2", datetime(2022, 5, 1), 400, "2022", "05", "01")
    ]

    schema = ["user_id", "timestamp", "amt", "partition_0", "partition_1", "partition_2"]

    input_spark_df = my_custom_spark_session.createDataFrame(data, schema)

    # Simulate materializing features for May 1st.
    output = user_recent_transactions.test_run(
        start_time=datetime(2022, 5, 1, tzinfo=timezone.utc),
        end_time=datetime(2022, 5, 2, tzinfo=timezone.utc),
        transactions=input_spark_df
    )

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2"],
        "amt_last_distinct_10_1h_10m": [["100", "200", "300"], ["400"]],
        # The result timestamp is rounded up to the nearest aggregation interval "end time". The aggregation interval
        # is ten minutes for this feature view.
        "timestamp": [(datetime(2022, 5, 1, 0, 10, tzinfo=timezone.utc))] * 2,
    })
    expected['timestamp'] = expected['timestamp'].astype('datetime64[us, UTC]')

    pandas.testing.assert_frame_equal(actual, expected,  check_like=True)


# Example showing of how to create your own spark session for testing instead of the Tecton provided
# tecton_pytest_spark_session.
@pytest.fixture(scope='module')
def my_custom_spark_session():
    """Returns a custom spark session configured for use in Tecton unit testing."""
    with resources.path("tecton_spark.jars", "tecton-udfs-spark-3.jar") as path:
        tecton_udf_jar_path = str(path)

    spark = (
        SparkSession.builder.appName("my_custom_spark_session")
        .config("spark.jars", tecton_udf_jar_path)
        # This short-circuit's Spark's attempt to auto-detect a hostname for the master address, which can lead to
        # errors on hosts with "unusual" hostnames that Spark believes are invalid.
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        tecton.set_tecton_spark_session(spark)
        yield spark
    finally:
        spark.stop()
