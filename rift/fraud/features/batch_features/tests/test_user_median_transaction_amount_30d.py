import os
import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd

from fraud.features.batch_features.user_median_transaction_amount_30d import user_median_transaction_amount_30d

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_median_transaction_amount_30d(tecton_pytest_spark_session):
    # Create test data with transactions for two users
    start_time = datetime(2022, 5, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=30)
    
    # Create transactions for two users with different amounts
    data = [
        (start_time, "user_1", 100.0),
        (start_time + timedelta(days=1), "user_1", 200.0),
        (start_time + timedelta(days=2), "user_1", 300.0),
        (start_time, "user_2", 50.0),
        (start_time + timedelta(days=1), "user_2", 150.0),
        (start_time + timedelta(days=2), "user_2", 250.0)
    ]
    
    # Define schema
    schema = ["timestamp", "user_id", "amt"]
    
    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)
    
    # Convert to pandas DataFrame
    input_pandas_df = input_spark_df.toPandas()
    
    # Run the transformation
    output = user_median_transaction_amount_30d.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions_batch": input_pandas_df}
    )
    
    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()
    output_pd["timestamp"] = pd.to_datetime(output_pd["timestamp"]).dt.tz_convert("UTC").astype('datetime64[us, UTC]')
    
    # Create expected output with rolling medians
    expected_data = [
        # user_1 transactions with rolling medians
        ("user_1", 100.0, start_time),  # First transaction, median = 100
        ("user_1", 150.0, start_time + timedelta(days=1)),  # After second transaction, median = (100 + 200) / 2
        ("user_1", 200.0, start_time + timedelta(days=2)),  # After third transaction, median = 200 (middle value)
        
        # user_2 transactions with rolling medians
        ("user_2", 50.0, start_time),  # First transaction, median = 50
        ("user_2", 100.0, start_time + timedelta(days=1)),  # After second transaction, median = (50 + 150) / 2
        ("user_2", 150.0, start_time + timedelta(days=2))  # After third transaction, median = 150 (middle value)
    ]
    
    expected_pd = pd.DataFrame(expected_data, columns=["user_id", "amt", "timestamp"])
    expected_pd["timestamp"] = pd.to_datetime(expected_pd["timestamp"]).astype('datetime64[us, UTC]')
    
    # Sort both dataframes by user_id and timestamp to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected_pd = expected_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    
    # Assert the dataframes are equal
    pd.testing.assert_frame_equal(output_pd, expected_pd) 