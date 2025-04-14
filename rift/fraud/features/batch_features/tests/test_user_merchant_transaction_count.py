import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_merchant_transaction_count import user_merchant_transaction_count

def test_user_merchant_transaction_count():
    # Create test data with transactions
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 4, 13, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 5, 14, 0, tzinfo=timezone.utc)
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2', 'user3'],
        'merchant': ['merchant1', 'merchant1', 'merchant2', 'merchant1', 'merchant3', 'merchant2'],
        'timestamp': pd.Series([
            datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 4, 13, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 5, 14, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 5, 15, 0, tzinfo=timezone.utc)
        ]).dt.tz_localize(None).astype('datetime64[ns]').dt.tz_localize('UTC'),
        'amount': [100.0, 150.0, 200.0, 150.0, 300.0, 250.0]
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 6, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_merchant_transaction_count.run_transformation(start_time, end_time, mock_inputs={'transactions_batch': test_data})
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'merchant', 'timestamp', 'count'}

    # Verify the data transformation
    assert len(result_df) == 6  # Should have same number of rows as input

    # Verify user1's transactions
    user1_transactions = result_df[result_df['user_id'] == 'user1']
    assert len(user1_transactions) == 3
    assert len(user1_transactions[user1_transactions['merchant'] == 'merchant1']) == 2  # 2 transactions with merchant1
    assert len(user1_transactions[user1_transactions['merchant'] == 'merchant2']) == 1  # 1 transaction with merchant2

    # Verify user2's transactions
    user2_transactions = result_df[result_df['user_id'] == 'user2']
    assert len(user2_transactions) == 2
    assert len(user2_transactions[user2_transactions['merchant'] == 'merchant3']) == 1  # 1 transaction with merchant3

    # Verify timestamps are preserved
    result_df['timestamp'] = result_df['timestamp'].astype('datetime64[ns, UTC]')
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    ) 