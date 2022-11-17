from datetime import timedelta, datetime
from tecton import StreamFeatureView, FilteredSource
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import click_event_source


keyword_push_fv = StreamFeatureView(
    name="keyword_push_fv",
    source=FilteredSource(click_event_source),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 10, 10),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    description='The count of ad clicks for a user'
)