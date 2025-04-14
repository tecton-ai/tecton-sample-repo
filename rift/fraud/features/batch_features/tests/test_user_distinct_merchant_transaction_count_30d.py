import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d

def test_user_distinct_merchant_transaction_count_30d():
    # Create test data with transactions to different merchants
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),   # User1-Merchant1
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),   # User1-Merchant1 (duplicate)
        datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),   # User1-Merchant2
        datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),   # User1-Merchant3
        datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),   # User2-Merchant1
        datetime(2024, 1, 5, 10, 0, tzinfo=timezone.utc),   # User2-Merchant2
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user1', 'user2', 'user2'],
        'merchant': ['merchant1', 'merchant1', 'merchant2', 'merchant3', 'merchant1', 'merchant2'],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 6, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_distinct_merchant_transaction_count_30d.run_transformation(
        start_time,
        end_time,
        mock_inputs={'transactions_batch': test_data}
    )

    # Convert TectonDataFrame to pandas DataFrame for easier testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'distinct_merchant_transaction_count_30d', 'timestamp'}

    # Verify the counts for each user
    user1_result = result_df[result_df['user_id'] == 'user1'].iloc[0]
    user2_result = result_df[result_df['user_id'] == 'user2'].iloc[0]

    # User1 has 3 distinct merchants (merchant1 appears twice but should be counted once)
    assert user1_result['distinct_merchant_transaction_count_30d'] == 3
    # User2 has 2 distinct merchants
    assert user2_result['distinct_merchant_transaction_count_30d'] == 2

    # Verify timestamps match the max timestamp from input data
    expected_timestamp = test_data['timestamp'].max()
    assert user1_result['timestamp'] == expected_timestamp
    assert user2_result['timestamp'] == expected_timestamp 