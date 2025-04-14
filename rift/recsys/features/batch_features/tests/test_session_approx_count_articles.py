import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone
from recsys.features.batch_features.session_features import session_approx_count_articles

def test_session_approx_count_articles():
    # Create sample session data with timezone-aware timestamps
    sessions_data = pd.DataFrame({
        'session': [1, 1, 1, 2, 2, 3],  # Using integer session IDs
        'aid': [1, 2, 3, 1, 4, 5],  # article IDs
        'ts': [
            datetime(2022, 8, 1, 12, 0, tzinfo=timezone.utc),  # session 1 articles
            datetime(2022, 8, 1, 12, 30, tzinfo=timezone.utc),
            datetime(2022, 8, 1, 13, 0, tzinfo=timezone.utc),
            datetime(2022, 8, 2, 12, 0, tzinfo=timezone.utc),  # session 2 articles
            datetime(2022, 8, 2, 12, 30, tzinfo=timezone.utc),
            datetime(2022, 8, 3, 12, 0, tzinfo=timezone.utc),  # session 3 article
        ]
    })

    # Run the transformation with start_time and end_time parameters
    start_time = datetime(2022, 8, 1, tzinfo=timezone.utc)  # Start of the first day
    end_time = datetime(2022, 8, 4, tzinfo=timezone.utc)    # One day after the last timestamp
    result = session_approx_count_articles.run_transformation(
        start_time,
        end_time,
        mock_inputs={'sessions_data': sessions_data}
    )

    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()

    # Verify the output schema
    expected_columns = ['session', 'aid', 'ts']
    assert all(col in result_df.columns for col in expected_columns)

    # Verify the data types
    assert result_df['session'].dtype == 'int32'  # Using int32 for session
    assert result_df['aid'].dtype == 'int32'     # Using int32 for aid
    assert pd.api.types.is_datetime64_any_dtype(result_df['ts'])

    # Verify the data is preserved correctly
    assert len(result_df) == len(sessions_data)
    assert set(result_df['session']) == set(sessions_data['session'])
    assert set(result_df['aid']) == set(sessions_data['aid']) 