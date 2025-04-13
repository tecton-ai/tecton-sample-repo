import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_transaction_counts import user_transaction_counts


def test_user_transaction_counts():
    # Create test data with transactions over different time periods
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),  # Within 1 day
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),  # Within 1 day
        datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),  # Within 30 days
        datetime(2024, 1, 30, 10, 0, tzinfo=timezone.utc),  # Within 30 days
        datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),  # Within 90 days
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2'],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 3, 2, 0, 0, tzinfo=timezone.utc)

    # Run transformation
    result = user_transaction_counts.run_transformation(start_time, end_time, mock_inputs={'transactions': test_data})
    result_df = result.to_pandas()

    # Verify output schema
    expected_columns = ['user_id', 'timestamp', 'transaction']
    assert all(col in result_df.columns for col in expected_columns)

    # Verify the data is correctly prepared for aggregation
    assert all(result_df['transaction'] == 1)  # All transactions are marked with 1 for counting
    assert len(result_df) == len(test_data)  # No rows should be lost
    assert all(result_df['user_id'] == test_data['user_id'])  # User IDs should match
    pd.testing.assert_series_equal(result_df['timestamp'], test_data['timestamp'])  # Timestamps should match