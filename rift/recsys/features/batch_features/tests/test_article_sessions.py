import pandas as pd
from datetime import datetime, timezone
from recsys.features.batch_features.article_features import article_sessions

def test_article_sessions():
    # Create test data with sessions interacting with articles over different time periods
    timestamps = [
        datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),   # Article1-Session1
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),   # Article1-Session2
        datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),  # Article2-Session1
        datetime(2024, 1, 30, 10, 0, tzinfo=timezone.utc),  # Article1-Session3
        datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),   # Article2-Session4 (outside 30d)
    ]

    test_data = pd.DataFrame({
        'session': [1, 2, 1, 3, 4],
        'aid': [1, 1, 2, 1, 2],
        'ts': pd.Series(timestamps).dt.tz_localize(None).astype('datetime64[us]').dt.tz_localize('UTC')
    })

    # Set time window to include all test data
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 3, 2, 0, 0, tzinfo=timezone.utc)

    # Run the transformation
    result = article_sessions.run_transformation(
        start_time,
        end_time,
        mock_inputs={'sessions_batch': test_data}
    )

    # Convert TectonDataFrame to pandas DataFrame for easier testing
    result_df = result.to_pandas()

    # Verify the output schema
    assert set(result_df.columns) == {'session', 'aid', 'ts'}

    # Verify the data types
    assert result_df['session'].dtype == 'int32'
    assert result_df['aid'].dtype == 'int32'
    assert pd.api.types.is_datetime64_any_dtype(result_df['ts'])

    # Verify no rows are lost in the transformation
    assert len(result_df) == len(test_data)

    # Verify article-session combinations are preserved
    for _, row in test_data.iterrows():
        session = row['session']
        aid = row['aid']
        timestamp = row['ts']
        
        # Find the corresponding row in the result
        matching_rows = result_df[
            (result_df['session'] == session) & 
            (result_df['aid'] == aid) &
            (result_df['ts'] == timestamp)
        ]
        
        # Verify we found exactly one matching row
        assert len(matching_rows) == 1

    # Verify timestamps are preserved
    pd.testing.assert_series_equal(
        result_df['ts'].sort_values().reset_index(drop=True),
        test_data['ts'].sort_values().reset_index(drop=True),
        check_names=False
    ) 