import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_median_transaction_amount_30d import user_median_transaction_amount_30d

def test_user_median_transaction_amount_30d():
    # Create test data with transactions of different amounts
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 4, 13, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 5, 14, 0, tzinfo=timezone.utc)
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2'],
        'amt': [100.0, 200.0, 300.0, 400.0, 500.0],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 6, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_median_transaction_amount_30d.run_transformation(start_time, end_time, mock_inputs={'transactions_batch': test_data})
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'timestamp', 'amt'}

    # Verify the data transformation
    assert len(result_df) == 5  # Should have same number of rows as input
    
    # Verify user1's transactions
    user1_transactions = result_df[result_df['user_id'] == 'user1']
    assert len(user1_transactions) == 3
    assert user1_transactions['amt'].median() == 200.0  # Median of [100, 200, 300]

    # Verify user2's transactions
    user2_transactions = result_df[result_df['user_id'] == 'user2']
    assert len(user2_transactions) == 2
    assert user2_transactions['amt'].median() == 450.0  # Median of [400, 500]

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    ) 