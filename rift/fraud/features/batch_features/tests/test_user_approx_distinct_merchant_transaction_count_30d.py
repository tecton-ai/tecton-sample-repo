import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_approx_distinct_merchant_transaction_count_30d import user_approx_distinct_merchant_transaction_count_30d

def test_user_approx_distinct_merchant_transaction_count_30d():
    # Create test data with transactions to different merchants
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),   # User1-Merchant1
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),   # User1-Merchant1 (duplicate)
        datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),   # User1-Merchant2
        datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),   # User1-Merchant3
        datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),   # User2-Merchant1
        datetime(2024, 1, 5, 10, 0, tzinfo=timezone.utc),   # User2-Merchant2
        datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),   # User1-Merchant4 (outside 30d window)
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user1', 'user2', 'user2', 'user1'],
        'merchant': ['merchant1', 'merchant1', 'merchant2', 'merchant3', 'merchant1', 'merchant2', 'merchant4'],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 3, 2, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_approx_distinct_merchant_transaction_count_30d.run_transformation(
        start_time,
        end_time,
        mock_inputs={'transactions_batch': test_data}
    )

    # Convert TectonDataFrame to pandas DataFrame for easier testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'timestamp', 'merchant'}

    # Verify no rows are lost in the transformation
    assert len(result_df) == len(test_data)

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

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    ) 