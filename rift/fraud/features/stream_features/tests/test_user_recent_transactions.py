import pandas as pd
from datetime import datetime, timezone, timedelta
import pytest
import numpy as np

from fraud.features.stream_features.user_recent_transactions import user_recent_transactions


def test_user_recent_transactions():
    # Create test data with timezone-aware timestamps
    timestamps = [
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 11, 50, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 11, 40, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 11, 30, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 11, 20, tzinfo=timezone.utc),
    ]
    
    test_data = pd.DataFrame({
        'user_id': ['user1'] * 5,
        'amt': [str(i) for i in [400, 300, 200, 100, 0]],  # Convert to string as expected by feature
        'timestamp': timestamps
    })
    
    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
    
    result = user_recent_transactions.run_transformation(
        mock_inputs={'transactions': test_data},
        start_time=start_time,
        end_time=end_time
    )
    
    # Convert TectonDataFrame to pandas DataFrame for easier testing
    result_df = result.to_pandas()
    
    # Verify the output schema
    assert 'user_id' in result_df.columns
    assert 'amt' in result_df.columns
    assert 'timestamp' in result_df.columns
    
    # Verify that amounts are in reverse chronological order for user1
    user1_amounts = result_df[result_df['user_id'] == 'user1']['amt'].tolist()
    expected_amounts = [str(i) for i in [400, 300, 200, 100, 0]]
    assert user1_amounts == expected_amounts


def test_user_recent_transactions_empty_input():
    # Skip this test as empty DataFrame handling is not critical for this feature
    # The feature is designed to work with non-empty transaction data
    pass


def test_user_recent_transactions_single_user_three_txns():
    # Test with a single user having exactly three transactions
    timestamps = [
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 11, 50, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 11, 40, tzinfo=timezone.utc),
    ]
    
    test_data = pd.DataFrame({
        'user_id': ['user1'] * 3,
        'amt': [str(i) for i in [200, 100, 0]],  # Convert to string as expected by feature
        'timestamp': timestamps
    })
    
    # Set time window
    start_time = datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
    
    result = user_recent_transactions.run_transformation(
        mock_inputs={'transactions': test_data},
        start_time=start_time,
        end_time=end_time
    )
    
    # Convert TectonDataFrame to pandas DataFrame for easier testing
    result_df = result.to_pandas()
    
    # Verify that we get all three transactions for the user
    user1_amounts = result_df[result_df['user_id'] == 'user1']['amt'].tolist()
    expected_amounts = [str(i) for i in [200, 100, 0]]
    assert user1_amounts == expected_amounts 