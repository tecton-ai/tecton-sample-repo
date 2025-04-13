import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd
from ads.features.batch_features.user_distinct_ad_count_7d import user_distinct_ad_count_7d

def test_user_distinct_ad_count_7d():
    # Create test data with multiple users and ads
    ad_impressions = pd.DataFrame({
        'user_uuid': ['user1', 'user1', 'user1', 'user2', 'user2', 'user3'],
        'ad_id': ['ad1', 'ad2', 'ad1', 'ad3', 'ad4', 'ad5'],
        'timestamp': [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            datetime(2023, 1, 2, tzinfo=timezone.utc),
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            datetime(2023, 1, 4, tzinfo=timezone.utc),
            datetime(2023, 1, 5, tzinfo=timezone.utc),
            datetime(2023, 1, 6, tzinfo=timezone.utc)
        ]
    })

    # Set up time window
    start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 7, tzinfo=timezone.utc)

    # Run transformation
    result = user_distinct_ad_count_7d.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'ad_impressions': ad_impressions}
    )

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify output schema
    assert set(result_df.columns) == {'user_id', 'distinct_ad_count', 'timestamp'}

    # Verify results
    result_df = result_df.sort_values('user_id')
    assert len(result_df) == 3  # Three users
    
    # Check user1 (saw 2 distinct ads)
    user1 = result_df[result_df['user_id'] == 'user1'].iloc[0]
    assert user1['distinct_ad_count'] == 2  # ad1 and ad2
    
    # Check user2 (saw 2 distinct ads)
    user2 = result_df[result_df['user_id'] == 'user2'].iloc[0]
    assert user2['distinct_ad_count'] == 2  # ad3 and ad4
    
    # Check user3 (saw 1 distinct ad)
    user3 = result_df[result_df['user_id'] == 'user3'].iloc[0]
    assert user3['distinct_ad_count'] == 1  # ad5

    # Verify timestamp
    expected_timestamp = end_time - timedelta(microseconds=1)
    assert all(result_df['timestamp'] == expected_timestamp)

def test_user_distinct_ad_count_7d_no_data_in_window():
    # Create test data with a single row outside the time window
    ad_impressions = pd.DataFrame({
        'user_uuid': ['user1'],
        'ad_id': ['ad1'],
        'timestamp': [datetime(2022, 1, 1, tzinfo=timezone.utc)]  # Way before our window
    })
    
    # Set up time window
    start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 7, tzinfo=timezone.utc)
    
    # Run transformation
    result = user_distinct_ad_count_7d.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'ad_impressions': ad_impressions}
    )
    
    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()
    
    # Verify empty result
    assert len(result_df) == 0
    assert set(result_df.columns) == {'user_id', 'distinct_ad_count', 'timestamp'}

def test_user_distinct_ad_count_7d_single_user():
    # Create test data with a single user seeing multiple ads
    ad_impressions = pd.DataFrame({
        'user_uuid': ['user1', 'user1', 'user1', 'user1'],
        'ad_id': ['ad1', 'ad2', 'ad3', 'ad1'],
        'timestamp': [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            datetime(2023, 1, 2, tzinfo=timezone.utc),
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            datetime(2023, 1, 4, tzinfo=timezone.utc)
        ]
    })

    # Set up time window
    start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 5, tzinfo=timezone.utc)
    
    # Run transformation
    result = user_distinct_ad_count_7d.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'ad_impressions': ad_impressions}
    )
    
    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()
    
    # Verify results
    assert len(result_df) == 1
    assert result_df.iloc[0]['user_id'] == 'user1'
    assert result_df.iloc[0]['distinct_ad_count'] == 3  # ad1, ad2, ad3 (ad1 is counted only once) 