import pandas as pd
import pytest
from datetime import datetime, timedelta
from recsys.features.batch_features.article_features import article_interactions

def test_article_interactions():
    start_time = datetime(2022, 7, 31)
    end_time = start_time + timedelta(days=30)

    data = [
        (start_time, 1, 1, "click"),
        (start_time + timedelta(days=1), 2, 1, "cart"),
        (start_time + timedelta(days=2), 3, 1, "order"),
        (start_time, 1, 2, "click"),
        (start_time + timedelta(days=1), 2, 2, "cart"),
    ]

    input_df = pd.DataFrame(data, columns=["ts", "interaction", "aid", "type"])
    input_df['ts'] = pd.to_datetime(input_df['ts']).dt.tz_localize('UTC')

    output = article_interactions.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={"sessions_batch": input_df}
    )

    output_pd = output.to_pandas()

    expected_data = [
        (1, 3, pd.Series([start_time + timedelta(days=2)]).dt.tz_localize('UTC').astype('datetime64[us, UTC]').iloc[0]),
        (2, 2, pd.Series([start_time + timedelta(days=1)]).dt.tz_localize('UTC').astype('datetime64[us, UTC]').iloc[0]),
    ]
    expected_df = pd.DataFrame(expected_data, columns=["aid", "interaction_approx_count_distinct_30d", "ts"])
    expected_df['ts'] = pd.to_datetime(expected_df['ts']).astype('datetime64[us, UTC]')
    pd.testing.assert_frame_equal(output_pd, expected_df) 