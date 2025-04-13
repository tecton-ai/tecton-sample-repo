import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_weekend_transaction_count_30d import user_weekend_transaction_count_30d

def test_user_weekend_transaction_count_30d():
    # Create test data with transactions on weekdays and weekends
    timestamps = [
        # Weekend transactions (Saturday)
        datetime(2024, 1, 6, 10, 0, tzinfo=timezone.utc),   # Saturday
        datetime(2024, 1, 6, 14, 0, tzinfo=timezone.utc),   # Saturday
        # Weekend transactions (Sunday)
        datetime(2024, 1, 7, 10, 0, tzinfo=timezone.utc),   # Sunday
        # Weekday transactions
        datetime(2024, 1, 8, 10, 0, tzinfo=timezone.utc),   # Monday
        datetime(2024, 1, 9, 10, 0, tzinfo=timezone.utc),   # Tuesday
        # Another weekend (Saturday)
        datetime(2024, 1, 13, 10, 0, tzinfo=timezone.utc),  # Saturday
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user2', 'user1', 'user2', 'user1'],
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 14, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_weekend_transaction_count_30d.run_transformation(start_time, end_time, mock_inputs={'transactions_batch': test_data})
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'is_weekend', 'timestamp'}

    # Verify weekend flag is set correctly
    weekend_transactions = result_df[result_df['is_weekend'] == 1]
    weekday_transactions = result_df[result_df['is_weekend'] == 0]

    # Check weekend transactions
    assert len(weekend_transactions) == 4  # 3 Saturday + 1 Sunday
    assert len(weekday_transactions) == 2  # 1 Monday + 1 Tuesday

    # Verify user-specific counts
    user1_weekends = weekend_transactions[weekend_transactions['user_id'] == 'user1']
    user2_weekends = weekend_transactions[weekend_transactions['user_id'] == 'user2']
    assert len(user1_weekends) == 3  # 2 Saturday + 1 Sunday
    assert len(user2_weekends) == 1  # 1 Sunday

    # Verify weekend days are correctly identified
    for _, row in weekend_transactions.iterrows():
        day_of_week = pd.Timestamp(row['timestamp']).dayofweek
        assert day_of_week in [5, 6], f"Expected weekend day (5 or 6), got {day_of_week}"

    for _, row in weekday_transactions.iterrows():
        day_of_week = pd.Timestamp(row['timestamp']).dayofweek
        assert day_of_week not in [5, 6], f"Expected weekday (0-4), got {day_of_week}"

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(result_df['timestamp'].sort_values(), test_data['timestamp'].sort_values()) 