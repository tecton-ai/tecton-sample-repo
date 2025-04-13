import pandas as pd
import numpy as np
from datetime import datetime, timezone
from tecton.types import Timestamp
from ads.features.batch_features.ad_auction_keywords import auction_keywords

def test_auction_keywords():
    # Create test data with timezone-aware timestamps
    test_time = datetime(2022, 5, 1, tzinfo=timezone.utc)
    input_data = pd.DataFrame({
        'auction_id': ['auction1', 'auction2', 'auction3'],
        'timestamp': [test_time] * 3,
        'content_keyword': ['bitcoin crypto currency', 'stock market news', 'bitcoin price']
    })
    
    # Run transformation with start_time and end_time parameters
    start_time = test_time
    end_time = datetime(2022, 5, 2, tzinfo=timezone.utc)
    result = auction_keywords.run_transformation(
        start_time=start_time,
        end_time=end_time,
        mock_inputs={'ad_impressions': input_data}
    )
    
    # Convert TectonDataFrame to pandas DataFrame for testing
    result_df = result.to_pandas()
    
    # Verify results
    assert len(result_df) == 3
    assert result_df['auction_id'].tolist() == ['auction1', 'auction2', 'auction3']
    
    # Check keyword lists
    expected_keywords = [
        ['bitcoin', 'crypto', 'currency'],
        ['stock', 'market', 'news'],
        ['bitcoin', 'price']
    ]
    for i, expected in enumerate(expected_keywords):
        actual = result_df['keyword_list'].iloc[i]
        assert np.array_equal(actual, expected), f"Keyword list mismatch at index {i}"
    
    # Check numeric values
    assert result_df['num_keywords'].tolist() == [3, 3, 2]
    assert result_df['keyword_contains_bitcoin'].tolist() == [True, False, True] 