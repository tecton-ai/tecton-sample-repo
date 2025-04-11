import os
from datetime import datetime, timedelta, timezone

import pandas
import pytest
import pytz

from fraud.features.batch_features.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a Tecton-defined PySpark session for testing
# Spark transformations and feature views. This session can be configured as needed by the user. If an entirely new
# session is needed, then you can create your own and set it with `tecton.set_tecton_spark_session()`.
@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_distinct_merchant_transaction_count_30d(tecton_pytest_spark_session):
    timestamps = [datetime(2022, 4, 10), datetime(2022, 4, 25), datetime(2022, 5, 1)]

    data = [
        (
            "user_1", "merchant_1", timestamps[0], timestamps[0].year, timestamps[0].month, timestamps[0].day
        ),
        (
            "user_1", "merchant_2", timestamps[1], timestamps[1].year, timestamps[1].month, timestamps[1].day
        ),
        (
            "user_2", "merchant_1", timestamps[2], timestamps[2].year, timestamps[2].month, timestamps[2].day
        ),
    ]

    schema = ["user_id", "merchant", "timestamp", "partition_0", "partition_1", "partition_2"]

    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)

    # Simulate materializing features for May 1st.
    output = user_distinct_merchant_transaction_count_30d.run_transformation(
        input_data={
            "start_time": datetime(2022, 5, 1),
            "end_time": datetime(2022, 5, 2),
            "transactions_batch": input_spark_df
        }
    )

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2"],
        "timestamp": [datetime(2022, 5, 2, tzinfo=timezone.utc) - timedelta(microseconds=1), datetime(2022, 5, 2, tzinfo=timezone.utc) - timedelta(microseconds=1)],
        "distinct_merchant_transaction_count_30d": [2, 1],
    })
    expected['timestamp'] = expected['timestamp'].astype('datetime64[us, UTC]')

    pandas.testing.assert_frame_equal(actual, expected,  check_like=True)


@pytest.fixture(scope='module', autouse=True)
def configure_spark_session(tecton_pytest_spark_session):
    # Custom configuration for the spark session. In this case, configure the timezone so that we don't need to specify
    # a timezone for every datetime in the mock data.
    tecton_pytest_spark_session.conf.set("spark.sql.session.timeZone", "UTC")
