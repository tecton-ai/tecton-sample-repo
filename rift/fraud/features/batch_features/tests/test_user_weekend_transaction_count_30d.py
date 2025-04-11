import pytest
from datetime import datetime, timedelta
import pandas as pd

from fraud.features.batch_features.user_weekend_transaction_count_30d import user_weekend_transaction_count_30d

def test_user_weekend_transaction_count_30d():
    # Create test data with transactions for two users
    start_time = datetime(2022, 5, 1)  # May 1, 2022 is a Sunday
    end_time = start_time + timedelta(days=30)
    
    # Create transactions for two users with different weekend counts
    data = [
        (start_time, "user_1", 100.0),  # Sunday
        (start_time + timedelta(days=1), "user_1", 200.0),  # Monday
        (start_time + timedelta(days=6), "user_1", 300.0),  # Saturday
        (start_time + timedelta(days=7), "user_1", 400.0),  # Sunday
        (start_time, "user_2", 50.0),  # Sunday
        (start_time + timedelta(days=1), "user_2", 150.0),  # Monday
        (start_time + timedelta(days=2), "user_2", 250.0)  # Tuesday
    ]
    
    # Create pandas DataFrame directly with timezone-aware timestamps
    input_df = pd.DataFrame(data, columns=["timestamp", "user_id", "amount"])
    input_df['timestamp'] = pd.to_datetime(input_df['timestamp']).dt.tz_localize('UTC')
    
    # Run the transformation
    output = user_weekend_transaction_count_30d.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"transactions_batch": input_df}
    )
    
    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()
    
    # Create expected output with timezone-aware timestamps in microsecond precision
    expected_data = [
        ("user_1", 3, pd.Series([start_time + timedelta(days=7)]).dt.tz_localize('UTC').astype('datetime64[us, UTC]').iloc[0]),  # 3 weekend transactions (2 Sundays, 1 Saturday)
        ("user_2", 1, pd.Series([start_time + timedelta(days=2)]).dt.tz_localize('UTC').astype('datetime64[us, UTC]').iloc[0])  # 1 weekend transaction (1 Sunday)
    ]
    
    expected_df = pd.DataFrame(expected_data, columns=["user_id", "weekend_transaction_count_30d", "timestamp"])
    
    # Convert expected timestamps to microsecond precision
    expected_df['timestamp'] = pd.to_datetime(expected_df['timestamp']).astype('datetime64[us, UTC]')

    # Assert that the output matches the expected result
    pd.testing.assert_frame_equal(output_pd, expected_df) 