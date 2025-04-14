import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
from ads.features.stream_features.user_impression_counts import user_impression_counts

def test_user_impression_counts_basic():
    # This test verifies the basic functionality of the impression counting feature view.
    # It ensures that impressions are correctly counted and timestamped, which is crucial
    # for frequency capping and user targeting decisions in the ad system.
    now = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    ad_impressions = pd.DataFrame({
        'user_uuid': ['user1', 'user1', 'user1'],
        'timestamp': [
            now - timedelta(minutes=30),
            now - timedelta(minutes=45),
            now - timedelta(minutes=55),
        ]
    })

    # Run transformation
    result = user_impression_counts.run_transformation(
        mock_inputs={'ad_impressions': ad_impressions},
        end_time=now,
        start_time=now - timedelta(hours=1)  # Start time 1 hour before end time
    )

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify schema matches the feature view definition
    assert set(result_df.columns) == {'user_id', 'timestamp', 'impression'}

    # Verify that each row has an impression value of 1
    assert all(result_df['impression'] == 1)
    assert result_df['impression'].dtype == np.int64  # Verify type is Int64

    # Verify that user_uuid is correctly mapped to user_id
    assert all(result_df['user_id'] == 'user1')

    # Verify all timestamps are preserved
    assert len(result_df) == len(ad_impressions)

def test_user_impression_counts_multiple_users():
    # Create test data with multiple users
    now = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    ad_impressions = pd.DataFrame({
        'user_uuid': ['user1', 'user2', 'user1'],
        'timestamp': [
            now - timedelta(minutes=30),
            now - timedelta(minutes=45),
            now - timedelta(minutes=55),
        ]
    })

    # Run transformation
    result = user_impression_counts.run_transformation(
        mock_inputs={'ad_impressions': ad_impressions},
        end_time=now,
        start_time=now - timedelta(hours=1)  # Start time 1 hour before end time
    )

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify that each row has an impression value of 1
    assert all(result_df['impression'] == 1)

    # Verify user counts
    user1_rows = result_df[result_df['user_id'] == 'user1']
    user2_rows = result_df[result_df['user_id'] == 'user2']
    assert len(user1_rows) == 2  # user1 has 2 impressions
    assert len(user2_rows) == 1  # user2 has 1 impression

def test_user_impression_counts_time_windows():
    # This test verifies that impressions are correctly counted across different time windows.
    # The feature view uses these counts for frequency capping and user targeting decisions,
    # so it's critical that the time window logic works correctly.
    now = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    ad_impressions = pd.DataFrame({
        'user_uuid': ['user1'] * 4,
        'timestamp': [
            now - timedelta(minutes=30),   # within 1h
            now - timedelta(minutes=59),   # within 1h
            now - timedelta(hours=23),     # within 24h
            now - timedelta(hours=23, minutes=30),  # within 24h
        ]
    })

    # Run transformation
    result = user_impression_counts.run_transformation(
        mock_inputs={'ad_impressions': ad_impressions},
        end_time=now,
        start_time=now - timedelta(days=1)  # Start time 1 day before end time
    )

    # Convert to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify that each row has an impression value of 1
    assert all(result_df['impression'] == 1)

    # Verify that all rows are for user1
    assert all(result_df['user_id'] == 'user1')

    # Verify timestamps are preserved
    assert len(result_df) == len(ad_impressions) 