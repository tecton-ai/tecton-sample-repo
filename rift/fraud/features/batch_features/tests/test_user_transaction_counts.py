import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_transaction_counts import user_transaction_counts


def test_user_transaction_counts():
    # Create test data with transactions
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 4, 13, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 5, 14, 0, tzinfo=timezone.utc)
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2'],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 6, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_transaction_counts.run_transformation(start_time, end_time, mock_inputs={'transactions_batch': test_data})
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'timestamp', 'count'}

    # Verify the data transformation
    assert len(result_df) == 5  # Should have same number of rows as input

    # Verify user1's transactions
    user1_transactions = result_df[result_df['user_id'] == 'user1']
    assert len(user1_transactions) == 3
    assert user1_transactions['count'].sum() == 3  # Total count for user1

    # Verify user2's transactions
    user2_transactions = result_df[result_df['user_id'] == 'user2']
    assert len(user2_transactions) == 2
    assert user2_transactions['count'].sum() == 2  # Total count for user2

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    )