import pytest
from datetime import datetime, timedelta
from ads.features.stream_features.user_click_counts_push import user_click_counts_wafv
from tecton import TimeWindow

def test_user_click_counts_push_configuration():
    # Verify feature view name
    assert user_click_counts_wafv.name == "user_click_counts_wafv"
    
    # Verify online/offline settings
    assert user_click_counts_wafv.online is True
    assert user_click_counts_wafv.offline is True
    
    # Verify feature start time
    expected_start_time = datetime(2023, 1, 1).replace(tzinfo=user_click_counts_wafv.feature_start_time.tzinfo)
    assert user_click_counts_wafv.feature_start_time == expected_start_time
    
    # Verify alert email
    assert user_click_counts_wafv.alert_email == "demo-user@tecton.ai"
    
    # Verify timestamp field
    assert user_click_counts_wafv.timestamp_field == 'timestamp'
    
    # Verify tags
    assert user_click_counts_wafv.tags == {'release': 'production'}
    
    # Verify owner
    assert user_click_counts_wafv.owner == 'demo-user@tecton.ai'
    
    # Verify description
    assert user_click_counts_wafv.description == 'The count of ad clicks for a user' 