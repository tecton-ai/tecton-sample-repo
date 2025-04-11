import pytest
from datetime import datetime, timedelta
from ads.features.stream_features.content_keyword_clicks_push import content_keyword_click_counts_push
from tecton import BatchTriggerType

def test_content_keyword_clicks_push_configuration():
    # Verify feature view name
    assert content_keyword_click_counts_push.name == "keyword_clicks_fv"
    
    # Verify online/offline settings
    assert content_keyword_click_counts_push.online is True
    assert content_keyword_click_counts_push.offline is True
    
    # Verify feature start time
    expected_start_time = datetime(2023, 1, 1).replace(tzinfo=content_keyword_click_counts_push.feature_start_time.tzinfo)
    assert content_keyword_click_counts_push.feature_start_time == expected_start_time
    
    # Verify batch schedule
    assert content_keyword_click_counts_push.batch_schedule == timedelta(days=1)
    
    # Verify manual trigger backfill end time
    expected_backfill_time = datetime(2023, 5, 1).replace(tzinfo=content_keyword_click_counts_push.manual_trigger_backfill_end_time.tzinfo)
    assert content_keyword_click_counts_push.manual_trigger_backfill_end_time == expected_backfill_time
    
    # Verify TTL
    assert content_keyword_click_counts_push.ttl == timedelta(days=30)
    
    # Verify tags
    assert content_keyword_click_counts_push.tags == {'release': 'production'}
    
    # Verify owner
    assert content_keyword_click_counts_push.owner == 'demo-user@tecton.ai'
    
    # Verify description
    assert content_keyword_click_counts_push.description == 'The ad clicks for a content keyword'
    
    # Verify batch trigger type
    assert content_keyword_click_counts_push.batch_trigger == BatchTriggerType.MANUAL
    
    # Verify timestamp field
    assert content_keyword_click_counts_push.timestamp_field == 'timestamp'