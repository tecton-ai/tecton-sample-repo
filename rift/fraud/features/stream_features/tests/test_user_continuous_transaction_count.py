import os
import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np

from fraud.features.stream_features.user_continuous_transaction_count import user_continuous_transaction_count

@pytest.mark.skipif(os.environ.get("TECTON_TEST_SPARK") is None, reason="Requires JDK installation and $JAVA_HOME env variable to run, so we skip unless user sets the `TECTON_TEST_SPARK` env var.")
def test_user_continuous_transaction_count(tecton_pytest_spark_session):
    # Create test data with transactions over time
    start_time = datetime(2022, 5, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=2)  # Ensure we cover the 1-hour window
    
    # Create transactions for two users with specific patterns:
    # user_1: Multiple transactions within different time windows
    # user_2: Fewer transactions spread out
    data = [
        # user_1 transactions
        (start_time, "user_1", "txn1", "retail", 100.0, 0, "merchant1", 40.7, -74.0),  # Within 1min window
        (start_time + timedelta(seconds=30), "user_1", "txn2", "retail", 200.0, 0, "merchant2", 40.7, -74.0),  # Within 1min window
        (start_time + timedelta(minutes=2), "user_1", "txn3", "retail", 300.0, 0, "merchant3", 40.7, -74.0),  # Outside 1min, within 30min
        (start_time + timedelta(minutes=35), "user_1", "txn4", "retail", 400.0, 0, "merchant4", 40.7, -74.0),  # Outside 30min, within 1h
        (start_time + timedelta(minutes=50), "user_1", "txn5", "retail", 500.0, 0, "merchant5", 40.7, -74.0),  # Within 1h
        
        # user_2 transactions
        (start_time + timedelta(seconds=45), "user_2", "txn6", "retail", 150.0, 0, "merchant6", 40.7, -74.0),  # Within 1min window
        (start_time + timedelta(minutes=25), "user_2", "txn7", "retail", 250.0, 0, "merchant7", 40.7, -74.0),  # Within 30min window
        (start_time + timedelta(minutes=55), "user_2", "txn8", "retail", 350.0, 0, "merchant8", 40.7, -74.0),  # Within 1h
    ]
    
    # Define schema
    schema = ["timestamp", "user_id", "transaction_id", "category", "amt", "is_fraud", "merchant", "merch_lat", "merch_long"]
    
    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)
    
    # Convert to pandas DataFrame
    input_pandas_df = input_spark_df.toPandas()
    
    # Run the transformation
    output = user_continuous_transaction_count.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions": input_pandas_df}
    )
    
    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()
    output_pd["timestamp"] = pd.to_datetime(output_pd["timestamp"]).dt.tz_convert("UTC").astype('datetime64[us, UTC]')
    
    # Create expected output with individual transactions
    expected_data = [
        # user_1 transactions
        ("user_1", np.int32(1), start_time),  # First transaction
        ("user_1", np.int32(1), start_time + timedelta(seconds=30)),  # Second transaction
        ("user_1", np.int32(1), start_time + timedelta(minutes=2)),  # Third transaction
        ("user_1", np.int32(1), start_time + timedelta(minutes=35)),  # Fourth transaction
        ("user_1", np.int32(1), start_time + timedelta(minutes=50)),  # Fifth transaction
        
        # user_2 transactions
        ("user_2", np.int32(1), start_time + timedelta(seconds=45)),  # First transaction
        ("user_2", np.int32(1), start_time + timedelta(minutes=25)),  # Second transaction
        ("user_2", np.int32(1), start_time + timedelta(minutes=55)),  # Third transaction
    ]
    
    expected_pd = pd.DataFrame(expected_data, columns=["user_id", "transaction", "timestamp"])
    expected_pd["timestamp"] = pd.to_datetime(expected_pd["timestamp"]).astype('datetime64[us, UTC]')
    
    # Sort both dataframes by user_id and timestamp to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected_pd = expected_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    
    # Assert the dataframes are equal
    pd.testing.assert_frame_equal(output_pd, expected_pd) 