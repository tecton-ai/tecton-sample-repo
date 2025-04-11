import os
import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd

from fraud.features.stream_features.last_transaction_amount_sql import last_transaction_amount_sql

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_last_transaction_amount_sql(tecton_pytest_spark_session):
    # Create test data with transactions over time
    start_time = datetime(2022, 5, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=2)  # Ensure we cover enough time
    
    # Create transactions for two users with specific patterns:
    # user_1: Multiple transactions with increasing amounts
    # user_2: Multiple transactions with varying amounts
    data = [
        # user_1 transactions
        (start_time, "user_1", 100.0),
        (start_time + timedelta(minutes=30), "user_1", 200.0),
        (start_time + timedelta(hours=1), "user_1", 300.0),  # This should be the last amount
        
        # user_2 transactions
        (start_time + timedelta(minutes=15), "user_2", 150.0),
        (start_time + timedelta(minutes=45), "user_2", 250.0),  # This should be the last amount
        (start_time + timedelta(minutes=20), "user_2", 50.0),  # Earlier timestamp, shouldn't be last
    ]

    # Define schema
    schema = ["timestamp", "user_id", "amt"]
    
    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)

    # Convert to pandas DataFrame
    input_pandas_df = input_spark_df.toPandas()

    # Get features in range directly from the feature view
    output = last_transaction_amount_sql.get_features_in_range(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions": input_pandas_df}
    )

    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()

    # Sort by user_id and _valid_from to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "_valid_from"]).reset_index(drop=True)

    # Check that the output has the expected columns
    expected_columns = ['user_id', 'amt', '_valid_from', '_valid_to']
    assert set(output_pd.columns) == set(expected_columns), f"Expected columns {expected_columns}, got {output_pd.columns}"

    # Check that we have data for both users
    assert len(output_pd) > 0, "Expected non-empty output"
    assert set(output_pd['user_id'].unique()) == {'user_1', 'user_2'}, "Expected data for both users"

    # Check specific values for user_1
    user1_data = output_pd[output_pd['user_id'] == 'user_1'].sort_values('_valid_from')
    assert len(user1_data) >= 3, "Expected at least 3 rows for user_1"
    assert user1_data.iloc[0]['amt'] == 100.0, "Expected first amount of 100.0 for user_1"
    assert user1_data.iloc[1]['amt'] == 200.0, "Expected second amount of 200.0 for user_1"
    assert user1_data.iloc[2]['amt'] == 300.0, "Expected last amount of 300.0 for user_1"

    # Check specific values for user_2
    user2_data = output_pd[output_pd['user_id'] == 'user_2'].sort_values('_valid_from')
    assert len(user2_data) >= 3, "Expected at least 3 rows for user_2"
    assert user2_data.iloc[0]['amt'] == 150.0, "Expected first amount of 150.0 for user_2"
    assert user2_data.iloc[1]['amt'] == 50.0, "Expected second amount of 50.0 for user_2"
    assert user2_data.iloc[2]['amt'] == 250.0, "Expected last amount of 250.0 for user_2" 