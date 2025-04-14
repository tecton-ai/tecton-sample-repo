import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from ads.features.stream_features.user_ad_impression_counts import user_ad_impression_counts

def test_user_ad_impression_counts():
    # Create test data with timezone-aware timestamps
    test_data = pd.DataFrame({
        'user_uuid': ['user1', 'user1', 'user2', 'user2', 'user1'],
        'ad_id': [1, 2, 1, 2, 1],  # Using integer ad_ids
        'timestamp': [
            datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 10, 30, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 11, 0, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 11, 30, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 12, 0, tzinfo=timezone.utc)
        ]
    })

    # Run the transformation
    # Set end_time to include all data points
    start_time = datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 5, 1, 13, 0, tzinfo=timezone.utc)  # Extended to ensure last data point is included
    result = user_ad_impression_counts.run_transformation(start_time, end_time, mock_inputs={'ad_impressions': test_data})

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Check output columns
    expected_columns = ['user_id', 'ad_id', 'timestamp', 'impression']
    assert all(col in result_df.columns for col in expected_columns)

    # Check data integrity
    assert len(result_df) == len(test_data)  # Should preserve all rows
    assert all(result_df['user_id'] == test_data['user_uuid'])  # Check user_uuid was renamed to user_id
    assert all(result_df['ad_id'] == test_data['ad_id'])  # Check ad_id is preserved
    assert all(result_df['impression'] == 1)  # Check all impressions are 1
    assert all(result_df['timestamp'] == test_data['timestamp'])  # Check timestamps are preserved

def test_user_ad_impression_counts_empty_input():
    # Create a DataFrame with data outside the time window
    test_data = pd.DataFrame({
        'user_uuid': ['user1'],
        'ad_id': [1],
        'timestamp': [datetime(2022, 5, 1, 9, 0, tzinfo=timezone.utc)]  # Data before the time window
    })
    
    # Run the transformation with a time window that doesn't include the data
    start_time = datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 5, 1, 12, 0, tzinfo=timezone.utc)
    result = user_ad_impression_counts.run_transformation(start_time, end_time, mock_inputs={'ad_impressions': test_data})

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Check output columns
    expected_columns = ['user_id', 'ad_id', 'timestamp', 'impression']
    assert all(col in result_df.columns for col in expected_columns)

    # Check data integrity
    assert len(result_df) == 0  # Should have no rows since data is outside the time window 
