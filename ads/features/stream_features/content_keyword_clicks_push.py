from datetime import timedelta, datetime
from tecton import StreamFeatureView, FilteredSource
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import keyword_click_source

# Windowed aggregations of key-word grouped click count events using the Tecton Stream Ingest API.
#
# See the documentation:
# https://docs.tecton.ai/using-the-ingestion-api/#creating-a-stream-feature-view-with-a-push-source
content_keyword_click_counts_push = StreamFeatureView(
    name="keyword_clicks_fv",
    source=FilteredSource(keyword_click_source),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    manual_trigger_backfill_end_time=datetime(2023, 5, 1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The ad clicks for a content keyword'
)
