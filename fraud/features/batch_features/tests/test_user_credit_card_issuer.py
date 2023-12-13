import os
from datetime import datetime, timedelta, timezone

import pandas
import pytest

from fraud.features.batch_features.user_credit_card_issuer import user_credit_card_issuer


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a Tecton-defined PySpark session for testing
# Spark transformations and feature views. This session can be configured as needed by the user. If an entirely new
# session is needed, then you can create your own and set it with `tecton.set_tecton_spark_session()`.
@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_credit_card_issuer_run(tecton_pytest_spark_session):
    input_pandas_df = pandas.DataFrame({
        "user_id": ["user_1", "user_2", "user_3", "user_4"],
        "signup_timestamp": [datetime(2022, 5, 1, tzinfo=timezone.utc)] * 4,
        "cc_num": [1000000000000000, 4000000000000000, 5000000000000000, 6000000000000000],
    })
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pandas_df)

    # Simulate materializing features for May 1st.
    output = user_credit_card_issuer.test_run(
        start_time=datetime(2022, 5, 1),
        end_time=datetime(2022, 5, 2),
        fraud_users_batch=input_spark_df)

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2", "user_3", "user_4"],
        "signup_timestamp":  [datetime(2022, 5, 1)] * 4,
        "credit_card_issuer": ["other", "Visa", "MasterCard", "Discover"],
    })

    pandas.testing.assert_frame_equal(actual, expected)



@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_credit_card_issuer_ghf(tecton_pytest_spark_session):
    input_pandas_df = pandas.DataFrame({
        "user_id": ["user_1", "user_2", "user_3", "user_4"],
        "signup_timestamp": [datetime(2022, 5, 1, tzinfo=timezone.utc)] * 4,
        "cc_num": [1000000000000000, 4000000000000000, 5000000000000000, 6000000000000000],
    })
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pandas_df)

    spine_df = pandas.DataFrame({
        "user_id": ["user_1", "user_1", "user_2", "user_not_found"],
        "timestamp": [datetime(2022, 5, 1), datetime(2022, 5, 2), datetime(2022, 6, 1), datetime(2022, 6, 1)],
    })

    # Simulate offline retrieval for features computed based on the provided data source.
    output = user_credit_card_issuer.get_historical_features(spine_df, mock_inputs={"fraud_users_batch":input_spark_df})

    actual = output.to_pandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_1", "user_2", "user_not_found"],
        "timestamp": [datetime(2022, 5, 1), datetime(2022, 5, 2), datetime(2022, 6, 1), datetime(2022, 6, 1)],
        "user_credit_card_issuer__credit_card_issuer": [None, "other", "Visa", None],
    })

    # NOTE: because the Spark join has non-deterministic ordering, it is important to
    # sort the dataframe to avoid test flakes.
    actual = actual.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected = expected.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

    pandas.testing.assert_frame_equal(actual, expected)
