import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_merchant_transaction_count import user_merchant_transaction_count

def test_user_merchant_transaction_count():
    # Create test data with transactions between users and merchants
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),  # User1-Merchant1 Day1
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),  # User1-Merchant1 Day1
        datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc), # User1-Merchant2 Day15
        datetime(2024, 1, 30, 10, 0, tzinfo=timezone.utc), # User2-Merchant1 Day30
        datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),  # User2-Merchant2 Day60
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2', 'user2'],
        'merchant': ['merchant1', 'merchant1', 'merchant2', 'merchant1', 'merchant2'],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 3, 2, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_merchant_transaction_count.run_transformation(start_time, end_time, mock_inputs={'transactions': test_data})
    result_df = result.to_pandas()

    # Verify output schema
    expected_columns = ['user_id', 'merchant', 'timestamp', 'transaction']
    assert set(result_df.columns) == set(expected_columns)

    # Verify the data is correctly prepared for aggregation
    assert all(result_df['transaction'] == 1)  # All transactions are marked with 1 for counting
    assert len(result_df) == len(test_data)  # No rows should be lost

    # Verify user-merchant combinations are preserved
    for _, row in test_data.iterrows():
        user_id = row['user_id']
        merchant = row['merchant']
        timestamp = row['timestamp']
        
        # Find the corresponding row in the result
        matching_rows = result_df[
            (result_df['user_id'] == user_id) & 
            (result_df['merchant'] == merchant) &
            (result_df['timestamp'] == timestamp)
        ]
        
        # Verify we found exactly one matching row
        assert len(matching_rows) == 1
        
        # Verify the transaction value is 1
        assert matching_rows['transaction'].iloc[0] == 1

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    ) 