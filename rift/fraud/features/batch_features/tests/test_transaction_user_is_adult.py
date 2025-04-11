import os
import sys
import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta
from tecton import tecton_context

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from fraud.features.batch_features.transaction_user_is_adult import transaction_user_is_adult

def test_transaction_user_is_adult():
    # Create test data with transactions and users
    transaction_time = datetime(2022, 5, 1)
    start_time = transaction_time
    end_time = transaction_time + timedelta(days=1)
    
    # Create transactions for four users with different ages
    transaction_data = [
        (transaction_time, "user_1", 100.0),
        (transaction_time, "user_2", 200.0),
        (transaction_time, "user_3", 300.0),
        (transaction_time, "user_4", 400.0)
    ]
    
    # Create user data with different ages and signup timestamps
    user_data = [
        ("user_1", transaction_time - timedelta(days=19*365), transaction_time),  # 19 years old
        ("user_2", transaction_time - timedelta(days=17*365), transaction_time),  # 17 years old
        ("user_3", transaction_time - timedelta(days=18*365), transaction_time),  # 18 years old
        ("user_4", transaction_time - timedelta(days=30*365), transaction_time)   # 30 years old
    ]
    
    # Create pandas DataFrames directly
    transaction_df = pd.DataFrame(transaction_data, columns=["timestamp", "user_id", "amt"])
    user_df = pd.DataFrame(user_data, columns=["user_id", "dob", "signup_timestamp"])
    
    # Run the transformation
    output = transaction_user_is_adult.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={
            "transactions_batch": transaction_df,
            "fraud_users_batch": user_df
        }
    )
    
    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()
    # Convert timestamp to match expected precision
    output_pd["timestamp"] = pd.to_datetime(output_pd["timestamp"]).astype('datetime64[ns, UTC]')
    
    # Create expected output with is_adult flags and timezone-aware timestamps
    expected_data = [
        # Each transaction with its is_adult flag
        ("user_1", np.int32(1), pd.Timestamp(transaction_time).tz_localize('UTC')),  # 19 years old -> True
        ("user_2", np.int32(0), pd.Timestamp(transaction_time).tz_localize('UTC')),  # 17 years old -> False
        ("user_3", np.int32(1), pd.Timestamp(transaction_time).tz_localize('UTC')),  # 18 years old -> True
        ("user_4", np.int32(1), pd.Timestamp(transaction_time).tz_localize('UTC'))   # 30 years old -> True
    ]
    expected_pd = pd.DataFrame(expected_data, columns=["user_id", "user_is_adult", "timestamp"])
    
    # Sort both dataframes by user_id and timestamp to ensure consistent order
    output_pd = output_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected_pd = expected_pd.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    
    # Assert the dataframes are equal
    pd.testing.assert_frame_equal(output_pd, expected_pd) 