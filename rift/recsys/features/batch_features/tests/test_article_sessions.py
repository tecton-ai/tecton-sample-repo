import pytest
import pandas as pd
from datetime import datetime, timedelta
from recsys.features.batch_features.article_features import article_sessions

def test_article_sessions():
    start_time = datetime(2022, 7, 31)
    end_time = start_time + timedelta(days=7)

    data = [
        (start_time, 1, 1),
        (start_time + timedelta(days=1), 1, 2),
        (start_time + timedelta(days=2), 1, 3),
        (start_time, 2, 1),
        (start_time + timedelta(days=1), 2, 2),
    ]

    input_df = pd.DataFrame(data, columns=["ts", "aid", "session"])
    input_df['ts'] = pd.to_datetime(input_df['ts']).dt.tz_localize('UTC')

    output = article_sessions.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"sessions_batch": input_df}
    )

    output_pd = output.to_pandas()

    expected_data = [
        (1, 3, pd.Series([start_time + timedelta(days=2)]).dt.tz_localize('UTC').iloc[0]),
        (2, 2, pd.Series([start_time + timedelta(days=1)]).dt.tz_localize('UTC').iloc[0]),
    ]
    expected_df = pd.DataFrame(expected_data, columns=[
        "aid",
        "session",
        "ts"
    ])

    expected_df['ts'] = pd.to_datetime(expected_df['ts']).astype('datetime64[us, UTC]')
    expected_df['session'] = expected_df['session'].astype('int32')

    pd.testing.assert_frame_equal(output_pd, expected_df) 