import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from ads.features.stream_features.user_impression_counts import user_impression_counts

def test_user_impression_counts_multiple_users():
    # Create test data within a 1-hour window
    base_time = datetime(2022, 5, 1, 10, 0, tzinfo=pytz.UTC)
    test_data = pd.DataFrame({
        'user_uuid': ['user1', 'user1', 'user2'],
        'impression_id': ['imp1', 'imp2', 'imp3'],
        'timestamp': [
            base_time,
            base_time + timedelta(minutes=30),
            base_time + timedelta(minutes=45)
        ]
    })

    # Set time range for transformation
    start_time = base_time
    end_time = base_time + timedelta(hours=1)

    # Run the transformation
    result = user_impression_counts.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'ad_impressions': test_data}
    )

    # Convert result to pandas DataFrame for easier testing
    result_df = result.to_pandas()

    # Verify output columns
    assert set(result_df.columns) == {'user_id', 'impression', 'timestamp'}

    # Verify data integrity
    assert len(result_df) == len(test_data)
    assert all(result_df['user_id'].isin(test_data['user_uuid']))
    assert all(result_df['impression'] == 1)

def test_user_impression_counts_single_user():
    # Create test data with a single user
    base_time = datetime(2022, 5, 1, 10, 0, tzinfo=pytz.UTC)
    test_data = pd.DataFrame({
        'user_uuid': ['user1'],
        'impression_id': ['imp1'],
        'timestamp': [base_time]
    })

    # Set time range for transformation
    start_time = base_time
    end_time = base_time + timedelta(hours=1)

    # Run the transformation
    result = user_impression_counts.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'ad_impressions': test_data}
    )
    
    # Convert result to pandas DataFrame for easier testing
    result_df = result.to_pandas()
    
    # Verify output columns
    assert set(result_df.columns) == {'user_id', 'impression', 'timestamp'}
    
    # Verify data integrity
    assert len(result_df) == 1
    assert result_df['user_id'].iloc[0] == 'user1'
    assert result_df['impression'].iloc[0] == 1
    # Compare timestamps with timezone awareness
    result_time = pd.to_datetime(result_df['timestamp'].iloc[0])
    assert result_time.replace(tzinfo=pytz.UTC) == base_time 