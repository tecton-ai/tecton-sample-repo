import os
from datetime import datetime, timedelta

import pandas
import pytest

from fraud.features.batch_features.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a
# Tecton-defined PySpark session for testing Spark transformations and feature
# views.
@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_distinct_merchant_transaction_count_30d(tecton_pytest_spark_session):
    timestamps = [datetime(2022, 4, 10), datetime(2022, 4, 25), datetime(2022, 5, 1)]
    input_pandas_df = pandas.DataFrame({
        "user_id": ["user_1", "user_1", "user_2"],
        "merchant": ["merchant_1", "merchant_2", "merchant_1"],
        "timestamp": timestamps,
        "partition_0": [ts.year for ts in timestamps],
        "partition_1": [ts.month for ts in timestamps],
        "partition_2": [ts.day for ts in timestamps],
    })
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pandas_df)

    # Simulate materializing features for May 1st.
    output = user_distinct_merchant_transaction_count_30d.test_run(
        start_time=datetime(2022, 5, 1),
        end_time=datetime(2022, 5, 2),
        transactions_batch=input_spark_df
    )

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2"],
        "timestamp": [datetime(2022, 5, 2) - timedelta(microseconds=1), datetime(2022, 5, 2) - timedelta(microseconds=1)],
        "distinct_merchant_transaction_count_30d": [2, 1],
    })

    pandas.testing.assert_frame_equal(actual, expected)