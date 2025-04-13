import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import pytz
import tecton

from fraud.features.stream_features.user_recent_transactions import user_recent_transactions
from importlib import resources
from pyspark.sql import SparkSession


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


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a Tecton-defined PySpark session for testing
# Spark transformations and feature views. This session can be configured as needed by the user. If an entirely new
# session is needed, then you can create your own and set it with `tecton.set_tecton_spark_session()`.
@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_recent_transactions_spark(my_custom_spark_session):
    # Create test data with timezone-aware timestamps
    now = datetime.now(timezone.utc)
    test_data = pd.DataFrame({
        'user_id': ['user1'] * 5,
        'amt': ['100', '200', '300', '400', '500'],
        'timestamp': [
            now - timedelta(minutes=i*10)
            for i in range(5)
        ]
    })

    # Run the transformation
    result = user_recent_transactions.test_run(test_data)

    # Verify the output schema
    assert 'amt_last_distinct_10_1h' in result.columns
    assert 'user_id' in result.columns

    # Get the results for user1
    user1_results = result[result['user_id'] == 'user1']
    
    # Verify that amounts are in reverse chronological order
    expected_amounts = ['500', '400', '300', '200', '100']
    assert user1_results['amt_last_distinct_10_1h'].tolist() == expected_amounts


def test_user_recent_transactions_basic():
    # Create test data with timezone-aware timestamps
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),  # Most recent
        datetime(2024, 1, 1, 9, 55, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 9, 50, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 9, 45, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 9, 40, tzinfo=timezone.utc),  # Oldest
    ]

    # Create test DataFrame
    test_data = pd.DataFrame({
        'user_id': ['user1'] * 5,
        'amt': [str(i * 100) for i in range(5)],  # Amounts from 0 to 400
        'timestamp': pd.to_datetime(timestamps)  # Convert to pandas datetime
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc)  # Before earliest event
    end_time = datetime(2024, 1, 1, 10, 30, tzinfo=timezone.utc)   # After latest event

    # Run transformation
    result = user_recent_transactions.test_run(
        mock_inputs={'transactions': test_data},
        start_time=start_time,
        end_time=end_time
    )

    # Convert to pandas for easier assertions
    result = result.to_pandas()

    # Verify output schema
    assert 'user_id' in result.columns
    assert 'amt_last_distinct_10_1h' in result.columns
    assert 'timestamp' in result.columns

    # Verify data transformation
    user1_data = result[result['user_id'] == 'user1']
    assert len(user1_data) == 1  # Should have one row per user
    # Verify amounts are in reverse chronological order
    assert user1_data['amt_last_distinct_10_1h'].iloc[0] == ['500', '400', '300', '200', '100']


def test_user_recent_transactions_empty_input():
    # Test with empty input
    empty_data = pd.DataFrame({
        'user_id': [],
        'amt': [],
        'timestamp': []
    })

    result = user_recent_transactions.test_run(empty_data)
    assert 'amt_last_distinct_10_1h' in result.columns
    assert len(result) == 0


def test_user_recent_transactions_single_user():
    # Test with a single user with exactly three transactions
    now = datetime.now(timezone.utc)
    test_data = pd.DataFrame({
        'user_id': ['user1'] * 3,
        'amt': ['100', '200', '300'],
        'timestamp': [
            now - timedelta(minutes=i*10)
            for i in range(3)
        ]
    })

    result = user_recent_transactions.test_run(test_data)
    assert 'amt_last_distinct_10_1h' in result.columns
    
    # Get the results for user1
    user1_results = result[result['user_id'] == 'user1']
    
    # Verify that amounts are in reverse chronological order
    expected_amounts = ['300', '200', '100']
    assert user1_results['amt_last_distinct_10_1h'].tolist() == expected_amounts
