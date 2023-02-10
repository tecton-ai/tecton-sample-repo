from datetime import timedelta, datetime
from tecton import StreamFeatureView, FilteredSource
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import keyword_click_source

# Sample StreamFeatureView with PushSource
keyword_clicks_fv = StreamFeatureView(
    name="keyword_clicks_fv",
    source=FilteredSource(keyword_click_source),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    description='The ad clicks for a content keyword'
)
