import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.transaction_user_is_adult import transaction_user_is_adult

def test_transaction_user_is_adult():
    # This test verifies that the feature view correctly determines if a user is an adult
    # at the time of their transaction. This is important for fraud detection and
    # compliance with age-restricted transactions.
    transaction_timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
    ]

    # Create test data with users of different ages
    transactions = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user2'],
        'timestamp': pd.Series(transaction_timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # User1: Born in 2000 (24 years old in 2024)
    # User2: Born in 2010 (14 years old in 2024)
    users = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'dob': pd.Series([
            datetime(2000, 1, 1),
            datetime(2010, 1, 1)
        ]).astype('datetime64[us]'),
        'signup_timestamp': pd.Series([
            datetime(2023, 1, 1),
            datetime(2023, 1, 1)
        ]).astype('datetime64[us]')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)

    # Run transformation
    result = transaction_user_is_adult.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'transactions_batch': transactions, 'fraud_users_batch': users}
    )

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify output schema
    assert set(result_df.columns) == {'user_id', 'user_is_adult', 'timestamp'}

    # Verify the data transformation
    assert len(result_df) == 3  # Should have same number of rows as transactions

    # Verify user1's transactions (adult)
    user1_transactions = result_df[result_df['user_id'] == 'user1']
    assert len(user1_transactions) == 2
    assert all(user1_transactions['user_is_adult'] == 1)  # All transactions should be marked as adult

    # Verify user2's transactions (minor)
    user2_transactions = result_df[result_df['user_id'] == 'user2']
    assert len(user2_transactions) == 1
    assert all(user2_transactions['user_is_adult'] == 0)  # All transactions should be marked as minor

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        transactions['timestamp'].sort_values().reset_index(drop=True),
        check_names=False
    )

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3'],
        'timestamp': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']).astype('datetime64[ns]'),
        'birth_date': pd.to_datetime(['1990-01-01', '2010-01-01', '2000-01-01']).astype('datetime64[ns]'),
        'amount': [100.0, 200.0, 300.0]
    }) 