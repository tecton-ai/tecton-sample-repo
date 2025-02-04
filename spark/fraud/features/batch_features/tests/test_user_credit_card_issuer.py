import os
from datetime import datetime, timedelta, timezone

import pandas
import pytest
import pytz

from fraud.features.batch_features.user_credit_card_issuer import user_credit_card_issuer


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a Tecton-defined PySpark session for testing
# Spark transformations and feature views. This session can be configured as needed by the user. If an entirely new
# session is needed, then you can create your own and set it with `tecton.set_tecton_spark_session()`.
@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_credit_card_issuer_run(tecton_pytest_spark_session):
    # Same signup timestamp for all users
    signup_timestamp = datetime(2022, 5, 1)

    # Create the data
    data = [
        ("user_1", signup_timestamp, 1000000000000000),
        ("user_2", signup_timestamp, 4000000000000000),
        ("user_3", signup_timestamp, 5000000000000000),
        ("user_4", signup_timestamp, 6000000000000000)
    ]

    # Define schema: user_id, signup_timestamp, cc_num
    schema = ["user_id", "signup_timestamp", "cc_num"]

    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)

    # Simulate materializing features for May 1st.
    output = user_credit_card_issuer.test_run(
        start_time=datetime(2022, 5, 1),
        end_time=datetime(2022, 5, 2),
        fraud_users_batch=input_spark_df)

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2", "user_3", "user_4"],
        "signup_timestamp":  [datetime(2022, 5, 1, 0, 0, 0, 0, tzinfo=timezone.utc)] * 4,
        "credit_card_issuer": ["other", "Visa", "MasterCard", "Discover"],
    })
    expected['signup_timestamp'] = expected['signup_timestamp'].astype('datetime64[us, UTC]')

    pandas.testing.assert_frame_equal(actual, expected, check_like=True)



@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_credit_card_issuer_ghf(tecton_pytest_spark_session):
    signup_timestamp = datetime(2022, 5, 1)

    data = [
        ("user_1", signup_timestamp, 1000000000000000),
        ("user_2", signup_timestamp, 4000000000000000),
        ("user_3", signup_timestamp, 5000000000000000),
        ("user_4", signup_timestamp, 6000000000000000)
    ]

    # Define schema: user_id, signup_timestamp, cc_num
    schema = ["user_id", "signup_timestamp", "cc_num"]

    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)

    spine_df = pandas.DataFrame({
        "user_id": ["user_1", "user_1", "user_2", "user_not_found"],
        "timestamp": [datetime(2022, 5, 1), datetime(2022, 5, 2), datetime(2022, 6, 1), datetime(2022, 6, 1)],
    })

    # Simulate offline retrieval for features computed based on the provided data source.
    output = user_credit_card_issuer.get_historical_features(spine_df, mock_inputs={"fraud_users_batch":input_spark_df})

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_1", "user_2", "user_not_found"],
        "timestamp": [datetime(2022, 5, 1, tzinfo=timezone.utc), datetime(2022, 5, 2, tzinfo=timezone.utc), datetime(2022, 6, 1, tzinfo=timezone.utc), datetime(2022, 6, 1, tzinfo=timezone.utc)],
        "user_credit_card_issuer__credit_card_issuer": [None, "other", "Visa", None],
    })
    expected['timestamp'] = expected['timestamp'].astype('datetime64[us, UTC]')

    # NOTE: because the Spark join has non-deterministic ordering, it is important to
    # sort the dataframe to avoid test flakes.
    actual = actual.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected = expected.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

    pandas.testing.assert_frame_equal(actual, expected,  check_like=True)


@pytest.fixture(scope='module', autouse=True)
def configure_spark_session(tecton_pytest_spark_session):
    # Custom configuration for the spark session. In this case, configure the timezone so that we don't need to specify
    # a timezone for every datetime in the mock data.
    tecton_pytest_spark_session.conf.set("spark.sql.session.timeZone", "UTC")
