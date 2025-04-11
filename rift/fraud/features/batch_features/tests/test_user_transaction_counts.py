import pytest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from fraud.features.batch_features.user_transaction_counts import user_transaction_counts

def test_user_transaction_counts():
    # Create test data with transactions for two users
    start_time = datetime(2022, 5, 1)
    end_time = start_time + timedelta(days=30)
    
    # Create transactions for two users with different counts
    data = [
        (start_time, "user_1", 100.0),
        (start_time + timedelta(days=1), "user_1", 200.0),
        (start_time + timedelta(days=2), "user_1", 300.0),
        (start_time, "user_2", 50.0),
        (start_time + timedelta(days=1), "user_2", 150.0),
        (start_time + timedelta(days=2), "user_2", 250.0)
    ]
    
    # Create pandas DataFrame directly
    input_df = pd.DataFrame(data, columns=["timestamp", "user_id", "amount"])
    
    # Run the transformation
    output = user_transaction_counts.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions": input_df}
    )
    
    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()
    output_pd["timestamp"] = pd.to_datetime(output_pd["timestamp"]).dt.tz_convert("UTC").astype('datetime64[us, UTC]')
    
    # Create expected output with one row per transaction and a transaction column set to 1
    expected_data = [
        ("user_1", np.int32(1), start_time),
        ("user_1", np.int32(1), start_time + timedelta(days=1)),
        ("user_1", np.int32(1), start_time + timedelta(days=2)),
        ("user_2", np.int32(1), start_time),
        ("user_2", np.int32(1), start_time + timedelta(days=1)),
        ("user_2", np.int32(1), start_time + timedelta(days=2))
    ]
    
    expected_pd = pd.DataFrame(expected_data, columns=["user_id", "transaction", "timestamp"])
    expected_pd["timestamp"] = pd.to_datetime(expected_pd["timestamp"]).dt.tz_localize("UTC").astype('datetime64[us, UTC]')
    
    # Sort both dataframes by user_id and timestamp to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected_pd = expected_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    
    # Assert the dataframes are equal
    pd.testing.assert_frame_equal(output_pd, expected_pd) 