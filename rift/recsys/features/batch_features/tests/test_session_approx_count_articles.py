import pytest
import pandas as pd
from datetime import datetime, timedelta
from recsys.features.batch_features.article_features import article_sessions

def test_session_approx_count_articles():
    # Create test data with sessions for two articles
    start_time = datetime(2022, 7, 31)
    end_time = start_time + timedelta(days=30)

    # Create session events for two articles with different session counts
    data = [
        (start_time, 1, 1),  # Article 1, Session 1
        (start_time + timedelta(days=1), 1, 2),  # Article 1, Session 2
        (start_time + timedelta(days=2), 1, 3),  # Article 1, Session 3
        (start_time, 2, 1),  # Article 2, Session 1
        (start_time + timedelta(days=1), 2, 2),  # Article 2, Session 2
    ]

    # Create pandas DataFrame with timezone-aware timestamps
    input_df = pd.DataFrame(data, columns=["ts", "aid", "session"])
    input_df['ts'] = pd.to_datetime(input_df['ts']).dt.tz_localize('UTC')

    # Run the transformation
    output = article_sessions.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"sessions_batch": input_df}
    )

    # Convert output to pandas for easier assertion
    output_pd = output.to_pandas()

    # Create expected output with timezone-aware timestamps
    expected_data = [
        (1, 3, pd.Series([start_time + timedelta(days=2)]).dt.tz_localize('UTC').iloc[0]),  # Article 1: 3 sessions
        (2, 2, pd.Series([start_time + timedelta(days=1)]).dt.tz_localize('UTC').iloc[0]),  # Article 2: 2 sessions
    ]
    expected_df = pd.DataFrame(expected_data, columns=[
        "aid",
        "session",
        "ts"
    ])

    # Convert timestamps to match feature view output
    expected_df['ts'] = pd.to_datetime(expected_df['ts']).astype('datetime64[us, UTC]')
    # Convert session column to int32 to match feature view output
    expected_df['session'] = expected_df['session'].astype('int32')

    # Assert that the output matches the expected result
    pd.testing.assert_frame_equal(output_pd, expected_df) 