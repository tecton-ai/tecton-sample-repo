import pytest
import pandas as pd
from datetime import datetime, timezone
from fraud.features.batch_features.user_credit_card_issuer import user_credit_card_issuer

def test_user_credit_card_issuer():
    # Create test data with different credit card numbers
    test_data = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3', 'user4'],
        'signup_timestamp': [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            datetime(2024, 1, 3, tzinfo=timezone.utc),
            datetime(2024, 1, 4, tzinfo=timezone.utc)
        ],
        'cc_num': [
            4123456789012345,  # Visa
            5123456789012345,  # MasterCard
            6123456789012345,  # Discover
            7123456789012345   # Other
        ]
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 5, tzinfo=timezone.utc)

    # Run the transformation
    result = user_credit_card_issuer.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'fraud_users_batch': test_data}
    )
    result_df = result.to_pandas()

    # Verify output schema
    expected_columns = ['user_id', 'signup_timestamp', 'credit_card_issuer']
    assert set(result_df.columns) == set(expected_columns)

    # Verify the data is preserved correctly
    assert len(result_df) == len(test_data)

    # Expected credit card issuers based on first digit
    expected_issuers = {
        'user1': 'Visa',        # First digit: 4
        'user2': 'MasterCard',  # First digit: 5
        'user3': 'Discover',    # First digit: 6
        'user4': 'other'        # First digit: 7 (not mapped)
    }

    # Verify each user's credit card issuer is correctly mapped
    for user_id, expected_issuer in expected_issuers.items():
        matching_rows = result_df[result_df['user_id'] == user_id]
        assert len(matching_rows) == 1  # Should find exactly one row
        assert matching_rows.iloc[0]['credit_card_issuer'] == expected_issuer

    # Verify timestamps are preserved (ignoring precision differences)
    result_timestamps = result_df['signup_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z').sort_values().reset_index(drop=True)
    test_timestamps = test_data['signup_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z').sort_values().reset_index(drop=True)
    pd.testing.assert_series_equal(result_timestamps, test_timestamps) 