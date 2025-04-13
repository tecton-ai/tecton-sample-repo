import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from fraud.features.batch_features.user_home_location import user_home_location

def test_user_home_location():
    # Create test data with different user locations
    test_data = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3'],
        'signup_timestamp': [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            datetime(2024, 1, 3, tzinfo=timezone.utc)
        ],
        'lat': [37.7749, 40.7128, 51.5074],  # San Francisco, New York, London
        'long': [-122.4194, -74.0060, -0.1278]
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 4, tzinfo=timezone.utc)

    # Run the transformation
    result = user_home_location.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'fraud_users_batch': test_data}
    )
    result_df = result.to_pandas()

    # Verify output schema
    expected_columns = ['signup_timestamp', 'user_id', 'lat', 'long']
    assert set(result_df.columns) == set(expected_columns)

    # Verify the data is preserved correctly
    assert len(result_df) == len(test_data)

    # Verify each user's location is preserved
    for _, row in test_data.iterrows():
        user_id = row['user_id']
        lat = row['lat']
        long = row['long']
        
        # Find the corresponding row in the result
        matching_rows = result_df[result_df['user_id'] == user_id]
        
        # Verify we found exactly one matching row
        assert len(matching_rows) == 1
        
        # Verify the location values are preserved
        assert matching_rows.iloc[0]['lat'] == lat
        assert matching_rows.iloc[0]['long'] == long

    # Verify timestamps are preserved (ignoring precision differences)
    result_timestamps = result_df['signup_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z').sort_values().reset_index(drop=True)
    test_timestamps = test_data['signup_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z').sort_values().reset_index(drop=True)
    pd.testing.assert_series_equal(result_timestamps, test_timestamps) 