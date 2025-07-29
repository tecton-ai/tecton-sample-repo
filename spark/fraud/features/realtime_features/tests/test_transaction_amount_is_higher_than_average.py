from fraud.features.realtime_features.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
import os
import pytest
import pandas as pd
from datetime import datetime
import pytz

MOCK_VALUE = 42

# Testing the 'transaction_amount_is_higher_than_average' feature which takes in request data ('amt')
# and a precomputed feature ('amt_mean_1d_10m') as inputs

# To test Realtime Feature Views with Calculations, we use the get_features_for_events method,
# which evaluates the Calculation expressions on the input data.
# @pytest.mark.parametrize(
#     "daily_mean,amount,expected",
#     [
#         (100, 200.0, True),
#         (100, 10.0, False),
#         (100, 100.0, False),
#     ],
# )

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_transaction_amount_is_higher_than_average(tecton_pytest_spark_session):
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
    #
    #
    # input_df = pd.DataFrame({
    #     # add the required fields to run get_features_for_events on the realtime feature view
    #     'user_id': ['user123'],
    #     'timestamp': [pd.Timestamp('2023-01-01', tz='UTC')],
    #     'amt': [amount],
    #     'user_transaction_amount_metrics__amt_mean_1d_10m': [daily_mean],
    #     # add all the other rtfv's source's fields to mock the source entirely and skip executing part of the query tree
    #     # these fields are not used in the rtfv's features, so we provide a mock value for them
    #     'user_transaction_amount_metrics__amt_sum_1h_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_sum_1d_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_sum_3d_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_mean_1h_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_mean_3d_10m': [MOCK_VALUE],
    # })
    #
    # expected_df = pd.DataFrame({
    #     'user_id': ['user123'],
    #     'timestamp': [pd.Timestamp('2023-01-01', tz='UTC')],
    #     'amt': [amount],
    #     'user_transaction_amount_metrics__amt_mean_1d_10m': [daily_mean],
    #     'user_transaction_amount_metrics__amt_sum_1h_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_sum_1d_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_sum_3d_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_mean_1h_10m': [MOCK_VALUE],
    #     'user_transaction_amount_metrics__amt_mean_3d_10m': [MOCK_VALUE],
    #     # the expected feature value from the rtfv's Calculation
    #     'transaction_amount_is_higher_than_average__transaction_amount_is_higher_than_average': [expected]
    # }).astype({"timestamp": 'datetime64[us, UTC]'})
    #
    # actual_df = transaction_amount_is_higher_than_average.get_features_for_events(input_df).to_pandas()
    #
    # pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)

@pytest.fixture(scope='module', autouse=True)
def configure_spark_session(tecton_pytest_spark_session):
    # Custom configuration for the spark session. In this case, configure the timezone so that we don't need to specify
    # a timezone for every datetime in the mock data.
    tecton_pytest_spark_session.conf.set("spark.sql.session.timeZone", "UTC")