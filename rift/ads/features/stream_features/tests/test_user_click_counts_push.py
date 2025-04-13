import pandas as pd
from datetime import datetime, timezone
from ads.features.stream_features.user_click_counts_push import user_click_counts_push


def test_user_click_counts_push():
    # Create test data with various click values
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),  # Within 1 hour
        datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),   # Within 24 hours
        datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc),   # Within 72 hours
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),  # Within 1 hour
    ]

    test_data = pd.DataFrame({
        'user_id': ['user1', 'user1', 'user1', 'user2'],
        'clicked': [1, 1, 0, 1],  # Test both clicked and non-clicked events
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 7, 0, tzinfo=timezone.utc)  # 1 hour before earliest event
    end_time = datetime(2024, 1, 1, 10, 30, tzinfo=timezone.utc)  # 30 minutes after the latest event

    # Run transformation
    result = user_click_counts_push.run_transformation(start_time, end_time, mock_inputs={'user_event_source': test_data})

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'user_id', 'clicked', 'timestamp'}

    # Verify the data transformation
    assert result_df['user_id'].tolist() == ['user1', 'user1', 'user1', 'user2']
    assert result_df['clicked'].tolist() == [1, 1, 0, 1]  # Verify click values are preserved

    # Verify timestamps are preserved
    expected_timestamps = pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    pd.testing.assert_series_equal(
        result_df['timestamp'],
        expected_timestamps,
        check_names=False
    ) 