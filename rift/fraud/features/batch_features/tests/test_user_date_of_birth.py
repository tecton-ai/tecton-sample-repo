import pandas as pd
import pytest
from fraud.features.batch_features.user_date_of_birth import user_date_of_birth
from datetime import datetime, timezone

def test_user_date_of_birth():
    # Create test data with various date formats
    timestamps = [
        datetime(2022, 5, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2022, 5, 1, 10, 30, tzinfo=timezone.utc),
        datetime(2022, 5, 1, 11, 0, tzinfo=timezone.utc),
        datetime(2022, 5, 1, 11, 30, tzinfo=timezone.utc)
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3', 'user4'],
        'dob': ['1990-01-01', '01/15/1995', '15-02-2000', 'invalid'],
        'signup_timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2022, 5, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 5, 2, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = user_date_of_birth.run_transformation(start_time, end_time, mock_inputs={'fraud_users_batch': test_data})

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'user_date_of_birth', 'timestamp'}

    # Verify the data transformation
    assert len(result_df) == 3  # Should have 3 rows (invalid date dropped)
    
    # Verify date formatting for each user
    assert result_df.loc[result_df['user_id'] == 'user1', 'user_date_of_birth'].iloc[0] == '1990-01-01'  # Already in correct format
    assert result_df.loc[result_df['user_id'] == 'user2', 'user_date_of_birth'].iloc[0] == '1995-01-15'  # MM/DD/YYYY format
    assert result_df.loc[result_df['user_id'] == 'user3', 'user_date_of_birth'].iloc[0] == '2000-02-15'  # DD-MM-YYYY format

    # Verify timestamps are preserved (ignoring column names)
    pd.testing.assert_series_equal(
        result_df['timestamp'].sort_values().reset_index(drop=True),
        test_data['signup_timestamp'].iloc[:3].sort_values().reset_index(drop=True),
        check_names=False
    ) 