import os
import pytest
from datetime import datetime, timedelta
import pandas as pd
from datetime import timezone

from fraud.features.stream_features.user_transaction_amount_metrics import user_transaction_amount_metrics

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_transaction_amount_metrics(tecton_pytest_spark_session):
    # Create test data with transactions over time
    start_time = datetime(2022, 5, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=4)  # Ensure we cover the 3-day window

    # Create transactions for two users with specific patterns:
    # user_1: Multiple transactions within different time windows
    # user_2: Fewer transactions spread out
    data = [
        # user_1 transactions
        (start_time, "user_1", 100.0),  # Within 1h window
        (start_time + timedelta(minutes=30), "user_1", 200.0),  # Within 1h window
        (start_time + timedelta(hours=2), "user_1", 300.0),  # Outside 1h window, within 1d
        (start_time + timedelta(days=2), "user_1", 400.0),  # Within 3d window

        # user_2 transactions
        (start_time + timedelta(minutes=45), "user_2", 150.0),  # Within 1h window
        (start_time + timedelta(days=1), "user_2", 250.0),  # Within 3d window
    ]

    # Define schema
    schema = ["timestamp", "user_id", "amt"]

    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)

    # Convert to pandas DataFrame
    input_pandas_df = input_spark_df.toPandas()

    # Get features in range directly from the feature view
    output = user_transaction_amount_metrics.get_features_in_range(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions": input_pandas_df}
    )

    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()

    # Print output DataFrame info
    print("\nOutput DataFrame info:")
    print(output_pd.info())
    print("\nOutput DataFrame head:")
    print(output_pd.head())
    print("\nOutput DataFrame shape:", output_pd.shape)
    print("\nOutput DataFrame columns:", output_pd.columns.tolist())

    # Sort by user_id and _valid_from
    output_pd = output_pd.sort_values(["user_id", "_valid_from"]).reset_index(drop=True)

    # Convert timestamps to UTC and then to nanosecond precision
    output_pd["_valid_from"] = pd.to_datetime(output_pd["_valid_from"].dt.tz_convert("UTC").dt.floor("us")).astype("datetime64[ns, UTC]")
    output_pd["_valid_to"] = pd.to_datetime(output_pd["_valid_to"].dt.tz_convert("UTC").dt.floor("us")).astype("datetime64[ns, UTC]")

    # Check specific values for user_1
    user1_data = output_pd[output_pd["user_id"] == "user_1"]
    assert len(user1_data) > 0, "No data found for user_1"

    # Find the row after the third transaction (2 hours after start)
    user1_after_third = user1_data[user1_data["_valid_from"] == start_time + timedelta(hours=2)]
    assert len(user1_after_third) == 1, "Expected one row for user_1 after third transaction"
    assert user1_after_third["amt_sum_1h_continuous"].iloc[0] == 300.0  # Only includes the third transaction
    assert user1_after_third["amt_sum_1d_continuous"].iloc[0] == 600.0  # Includes all three transactions
    assert user1_after_third["amt_sum_3d_continuous"].iloc[0] == 600.0  # Includes all three transactions
    assert user1_after_third["amt_mean_1h_continuous"].iloc[0] == 300.0  # Only includes the third transaction
    assert user1_after_third["amt_mean_1d_continuous"].iloc[0] == 200.0  # Average of all three transactions
    assert user1_after_third["amt_mean_3d_continuous"].iloc[0] == 200.0  # Average of all three transactions

    # Find the row after the fourth transaction (2 days after start)
    user1_after_fourth = user1_data[user1_data["_valid_from"] == start_time + timedelta(days=2)]
    assert len(user1_after_fourth) == 1, "Expected one row for user_1 after fourth transaction"
    assert user1_after_fourth["amt_sum_1h_continuous"].iloc[0] == 400.0  # Only includes the fourth transaction
    assert user1_after_fourth["amt_sum_1d_continuous"].iloc[0] == 400.0  # Only includes the fourth transaction
    assert user1_after_fourth["amt_sum_3d_continuous"].iloc[0] == 1000.0  # Includes all four transactions
    assert user1_after_fourth["amt_mean_1h_continuous"].iloc[0] == 400.0  # Only includes the fourth transaction
    assert user1_after_fourth["amt_mean_1d_continuous"].iloc[0] == 400.0  # Only includes the fourth transaction
    assert user1_after_fourth["amt_mean_3d_continuous"].iloc[0] == 250.0  # Average of all four transactions

    # Check specific values for user_2
    user2_data = output_pd[output_pd["user_id"] == "user_2"]
    assert len(user2_data) > 0, "No data found for user_2"

    # Find the row after the first transaction (45 minutes after start)
    user2_after_first = user2_data[user2_data["_valid_from"] == start_time + timedelta(minutes=45)]
    assert len(user2_after_first) == 1, "Expected one row for user_2 after first transaction"
    assert user2_after_first["amt_sum_1h_continuous"].iloc[0] == 150.0  # Only includes the first transaction
    assert user2_after_first["amt_sum_1d_continuous"].iloc[0] == 150.0  # Only includes the first transaction
    assert user2_after_first["amt_sum_3d_continuous"].iloc[0] == 150.0  # Only includes the first transaction
    assert user2_after_first["amt_mean_1h_continuous"].iloc[0] == 150.0  # Only includes the first transaction
    assert user2_after_first["amt_mean_1d_continuous"].iloc[0] == 150.0  # Only includes the first transaction
    assert user2_after_first["amt_mean_3d_continuous"].iloc[0] == 150.0  # Only includes the first transaction

    # Find the row after the second transaction (1 day after start)
    user2_after_second = user2_data[user2_data["_valid_from"] == start_time + timedelta(days=1)]
    assert len(user2_after_second) == 1, "Expected one row for user_2 after second transaction"
    assert user2_after_second["amt_sum_1h_continuous"].iloc[0] == 250.0  # Only includes the second transaction
    assert user2_after_second["amt_sum_1d_continuous"].iloc[0] == 400.0  # Includes both transactions (150 + 250)
    assert user2_after_second["amt_sum_3d_continuous"].iloc[0] == 400.0  # Includes both transactions
    assert user2_after_second["amt_mean_1h_continuous"].iloc[0] == 250.0  # Only includes the second transaction
    assert user2_after_second["amt_mean_1d_continuous"].iloc[0] == 200.0  # Average of both transactions
    assert user2_after_second["amt_mean_3d_continuous"].iloc[0] == 200.0  # Average of both transactions 