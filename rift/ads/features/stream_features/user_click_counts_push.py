from datetime import timedelta, datetime
from tecton import StreamFeatureView, Aggregate
from tecton.types import Bool, Field, Int64
from ads.entities import user
from ads.data_sources.ad_impressions import user_click_push_source

# Windowed aggregations of user-keyed ad-click count events using the Tecton Stream Ingest API.
#
# See the documentation:
# https://docs.tecton.ai/using-the-ingestion-api/#creating-a-stream-feature-view-with-a-push-source
user_click_counts_wafv = StreamFeatureView(
    name="user_click_counts_wafv",
    source=user_click_push_source.unfiltered(),
    entities=[user],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    alert_email="demo-user@tecton.ai",
    features=[
        Aggregate(input_column=Field('clicked', Int64), function='count', time_window=timedelta(hours=1)),
        Aggregate(input_column=Field('clicked', Int64), function='count', time_window=timedelta(hours=24)),
        Aggregate(input_column=Field('clicked', Int64), function='count', time_window=timedelta(hours=72)),
    ],
    timestamp_field='timestamp',
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of ad clicks for a user',
    environment='tecton-core-1.1.0'
)
