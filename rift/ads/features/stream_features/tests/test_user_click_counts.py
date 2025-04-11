import pytest
import pandas as pd
from datetime import datetime, timezone
from ads.features.stream_features.user_click_counts import user_click_counts

def test_user_click_counts():
    # Create test data with timezone-aware timestamps
    test_data = pd.DataFrame({
        'user_uuid': ['user1', 'user1', 'user2', 'user2', 'user1'],
        'clicked': [1, 1, 0, 1, 1],
        'timestamp': [
            datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 10, 30, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 11, 0, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 11, 30, tzinfo=timezone.utc),
            datetime(2022, 5, 1, 12, 0, tzinfo=timezone.utc)
        ]
    })

    # Run the transformation with a time window that includes all data points
    start_time = datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 5, 1, 13, 0, tzinfo=timezone.utc)  # Extended to ensure last data point is included
    result = user_click_counts.run_transformation(start_time, end_time, mock_inputs={'ad_impressions': test_data})

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify output columns
    expected_columns = ['user_id', 'clicked', 'timestamp']
    assert all(col in result_df.columns for col in expected_columns)

    # Verify data integrity
    assert len(result_df) == len(test_data)
    assert all(result_df['user_id'] == test_data['user_uuid'])
    assert all(result_df['clicked'] == test_data['clicked'])
    assert all(result_df['timestamp'] == test_data['timestamp'])

def test_user_click_counts_empty_input():
    # Create a DataFrame with data outside the time window
    test_data = pd.DataFrame({
        'user_uuid': ['user1'],
        'clicked': [1],
        'timestamp': [datetime(2022, 5, 1, 9, 0, tzinfo=timezone.utc)]  # Data before the time window
    })
    
    # Run the transformation with a time window that doesn't include the data
    start_time = datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 5, 1, 12, 0, tzinfo=timezone.utc)
    result = user_click_counts.run_transformation(start_time, end_time, mock_inputs={'ad_impressions': test_data})

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify output columns
    expected_columns = ['user_id', 'clicked', 'timestamp']
    assert all(col in result_df.columns for col in expected_columns)

    # Verify data integrity
    assert len(result_df) == 0  # Should have no rows since data is outside the time window 