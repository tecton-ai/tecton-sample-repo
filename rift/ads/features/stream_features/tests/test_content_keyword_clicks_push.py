import pandas as pd
from datetime import datetime, timezone
from ads.features.stream_features.content_keyword_clicks_push import content_keyword_click_counts_push


def test_content_keyword_clicks_push():
    # Create test data with various click values
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),  # Within 1 minute
        datetime(2024, 1, 1, 9, 59, tzinfo=timezone.utc),  # Within 1 minute
        datetime(2024, 1, 1, 9, 57, tzinfo=timezone.utc),  # Within 5 minutes
        datetime(2024, 1, 1, 9, 56, tzinfo=timezone.utc),  # Within 5 minutes
    ]

    test_data = pd.DataFrame({
        'content_keyword': ['bitcoin', 'crypto', 'bitcoin', 'nft'],
        'clicked': [1, 1, 0, 1],  # Test both clicked and non-clicked events
        'timestamp': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 9, 55, tzinfo=timezone.utc)  # 1 minute before earliest event
    end_time = datetime(2024, 1, 1, 10, 1, tzinfo=timezone.utc)  # 1 minute after the latest event

    # Run transformation
    result = content_keyword_click_counts_push.run_transformation(start_time, end_time, mock_inputs={'keyword_click_source': test_data})

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'content_keyword', 'clicked', 'timestamp'}

    # Verify the data transformation
    assert result_df['content_keyword'].tolist() == ['bitcoin', 'crypto', 'bitcoin', 'nft']
    assert result_df['clicked'].tolist() == [1, 1, 0, 1]  # Verify click values are preserved

    # Verify timestamps are preserved
    expected_timestamps = pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    pd.testing.assert_series_equal(
        result_df['timestamp'],
        expected_timestamps,
        check_names=False
    ) 