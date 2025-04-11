import os
import pytest
from datetime import datetime, timedelta
import pandas as pd

from fraud.features.batch_features.user_credit_card_issuer import user_credit_card_issuer

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_credit_card_issuer(tecton_pytest_spark_session):
    # Create test data with users and their credit card numbers
    start_time = datetime(2022, 5, 1)
    end_time = start_time + timedelta(days=30)

    # Create user data with different credit card numbers
    data = [
        (start_time, "user_1", "4111111111111111"),  # Visa
        (start_time + timedelta(days=1), "user_2", "5555555555554444"),  # MasterCard
        (start_time + timedelta(days=2), "user_3", "6011111111111117"),  # Discover
        (start_time + timedelta(days=3), "user_4", "371449635398431")  # Other (Amex)
    ]

    # Create pandas DataFrame directly
    input_df = pd.DataFrame(data, columns=["signup_timestamp", "user_id", "cc_num"])

    # Run the transformation
    output = user_credit_card_issuer.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"fraud_users_batch": input_df}
    )

    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()

    # Create expected output with columns in the same order as the feature view output
    expected_data = [
        ("user_1", "Visa", start_time),
        ("user_2", "MasterCard", start_time + timedelta(days=1)),
        ("user_3", "Discover", start_time + timedelta(days=2)),
        ("user_4", "other", start_time + timedelta(days=3))
    ]

    expected_pd = pd.DataFrame(expected_data, columns=["user_id", "credit_card_issuer", "signup_timestamp"])
    expected_pd["signup_timestamp"] = pd.to_datetime(expected_pd["signup_timestamp"]).dt.tz_localize("UTC")
    expected_pd["signup_timestamp"] = expected_pd["signup_timestamp"].astype('datetime64[us, UTC]')

    # Sort both dataframes by user_id and timestamp to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "signup_timestamp"]).reset_index(drop=True)
    expected_pd = expected_pd.sort_values(["user_id", "signup_timestamp"]).reset_index(drop=True)

    # Assert the dataframes are equal
    pd.testing.assert_frame_equal(output_pd, expected_pd) 