import pytest
from datetime import datetime, timedelta
from ads.features.stream_features.content_keyword_click_counts import content_keyword_click_counts
from tecton import StreamProcessingMode

def test_content_keyword_click_counts_configuration():
    # Verify feature view configuration
    assert content_keyword_click_counts.timestamp_field == 'timestamp'
    assert content_keyword_click_counts.online is False
    assert content_keyword_click_counts.offline is False
    
    # Verify feature start time
    expected_start_time = datetime(2022, 5, 1).replace(tzinfo=content_keyword_click_counts.feature_start_time.tzinfo)
    assert content_keyword_click_counts.feature_start_time == expected_start_time
    
    # Verify tags
    assert content_keyword_click_counts.tags == {'release': 'production'}
    
    # Verify owner
    assert content_keyword_click_counts.owner == 'demo-user@tecton.ai'
    
    # Verify description
    assert content_keyword_click_counts.description == 'The count of ad impressions for a content_keyword' 