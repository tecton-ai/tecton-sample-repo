import os
import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd

from fraud.features.stream_features.user_recent_transactions import user_recent_transactions

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_recent_transactions(tecton_pytest_spark_session):
    # Create test data with transactions over time
    start_time = datetime(2022, 5, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=2)  # Ensure we cover the 1-hour window
    
    # Create transactions for two users with specific patterns:
    # user_1: Multiple transactions with some repeated amounts
    # user_2: Fewer transactions with unique amounts
    data = [
        # user_1 transactions
        (start_time, "user_1", 100.0),
        (start_time + timedelta(minutes=10), "user_1", 200.0),
        (start_time + timedelta(minutes=20), "user_1", 100.0),  # Repeated amount
        (start_time + timedelta(minutes=30), "user_1", 300.0),
        (start_time + timedelta(minutes=40), "user_1", 400.0),
        (start_time + timedelta(minutes=50), "user_1", 200.0),  # Repeated amount
        
        # user_2 transactions
        (start_time + timedelta(minutes=5), "user_2", 150.0),
        (start_time + timedelta(minutes=25), "user_2", 250.0),
        (start_time + timedelta(minutes=45), "user_2", 350.0)
    ]

    # Define schema
    schema = ["timestamp", "user_id", "amt"]
    
    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)

    # Convert to pandas DataFrame
    input_pandas_df = input_spark_df.toPandas()

    # Get features in range directly from the feature view
    output = user_recent_transactions.get_features_in_range(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions": input_pandas_df}
    )

    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()

    # Print debug information
    print("\nOutput DataFrame info:")
    print(output_pd.info())
    print("\nOutput DataFrame head:")
    print(output_pd.head())
    print("\nOutput DataFrame shape:", output_pd.shape)
    print("\nOutput DataFrame columns:", output_pd.columns.tolist())

    # Sort by user_id and _valid_from to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "_valid_from"]).reset_index(drop=True)

    # Check that the output has the expected columns
    expected_columns = ['user_id', 'amt_last_10_1h_continuous', '_valid_from', '_valid_to']
    assert set(output_pd.columns) == set(expected_columns), f"Expected columns {expected_columns}, got {output_pd.columns}"

    # Check that we have data for both users
    assert len(output_pd) > 0, "Expected non-empty output"
    assert set(output_pd['user_id'].unique()) == {'user_1', 'user_2'}, "Expected data for both users"

    # Print all rows for debugging
    print("\nAll rows for user_1:")
    print(output_pd[output_pd['user_id'] == 'user_1'].sort_values('_valid_from'))
    print("\nAll rows for user_2:")
    print(output_pd[output_pd['user_id'] == 'user_2'].sort_values('_valid_from'))

    # Check specific values for user_1
    user1_data = output_pd[output_pd['user_id'] == 'user_1'].sort_values('_valid_from')
    assert len(user1_data) >= 6, "Expected at least 6 rows for user_1"
    
    # Get the last row for user_1
    user1_last = user1_data.iloc[-1]
    print("\nUser 1's last row:")
    print(user1_last)

    # The last_10 should contain all amounts in the last hour in reverse chronological order
    expected_amounts = [200.0, 400.0, 300.0, 100.0, 200.0, 100.0]  # All transactions within the 1-hour window
    actual_amounts = user1_last['amt_last_10_1h_continuous']
    print("\nExpected amounts:", expected_amounts)
    print("Actual amounts:", actual_amounts)
    assert all(float(amt) == expected_amounts[i] for i, amt in enumerate(actual_amounts)), \
        f"Expected amounts {expected_amounts} in last_10, got {actual_amounts}"

    # Check specific values for user_2
    user2_data = output_pd[output_pd['user_id'] == 'user_2'].sort_values('_valid_from')
    assert len(user2_data) >= 3, "Expected at least 3 rows for user_2"
    
    # Get the last row for user_2
    user2_last = user2_data.iloc[-1]
    print("\nUser 2's last row:")
    print(user2_last)

    # The last_10 should contain all amounts in the last hour in reverse chronological order
    expected_amounts = [350.0, 250.0, 150.0]  # All transactions within the 1-hour window
    actual_amounts = user2_last['amt_last_10_1h_continuous']
    print("\nExpected amounts:", expected_amounts)
    print("Actual amounts:", actual_amounts)
    assert all(float(amt) == expected_amounts[i] for i, amt in enumerate(actual_amounts)), \
        f"Expected amounts {expected_amounts} in last_10, got {actual_amounts}" 