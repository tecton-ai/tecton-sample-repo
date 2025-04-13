import pandas as pd
import pytest
from fraud.features.batch_features.user_time_window_series_aggregation_features import user_transaction_sums
from datetime import datetime, timezone

def test_user_transaction_sums():
    # Create test data with transactions over 7 days
    timestamps = [
        datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2022, 5, 2, 11, 0, tzinfo=timezone.utc),
        datetime(2022, 5, 3, 10, 0, tzinfo=timezone.utc),
        datetime(2022, 5, 4, 12, 0, tzinfo=timezone.utc),
        datetime(2022, 5, 5, 11, 0, tzinfo=timezone.utc)
    ]
    
    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2'],
        'value': [100, 200, 300, 400, 500],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2022, 5, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 5, 6, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_transaction_sums.run_transformation(start_time, end_time, mock_inputs={'transactions': test_data})

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'timestamp', 'value'}

    # Verify the data transformation
    assert len(result_df) == 5  # Should have the same number of rows as input
    assert result_df['value'].sum() == 1500  # Sum of all values should be 1500

    # Verify user1's transactions
    user1_transactions = result_df[result_df['user_id'] == 'user1']
    assert len(user1_transactions) == 3
    assert user1_transactions['value'].sum() == 600  # 100 + 200 + 300

    # Verify user2's transactions
    user2_transactions = result_df[result_df['user_id'] == 'user2']
    assert len(user2_transactions) == 2
    assert user2_transactions['value'].sum() == 900  # 400 + 500 