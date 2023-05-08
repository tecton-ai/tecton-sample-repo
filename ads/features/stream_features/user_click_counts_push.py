from datetime import timedelta, datetime
from tecton import StreamFeatureView, Aggregation
from ads.entities import user
from ads.data_sources.ad_impressions import user_click_push_source

# Windowed aggregations of user-keyed ad-click count events using the Tecton Stream Ingest API.
#
# See the documentation:
# https://docs.tecton.ai/using-the-ingestion-api/#creating-a-stream-feature-view-with-a-push-source
user_click_counts_push = StreamFeatureView(
    name="user_click_counts_wafv",
    source=user_click_push_source,
    entities=[user],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    alert_email="pooja@tecton.ai",
    aggregations=[
        Aggregation(column='clicked', function='count', time_window=timedelta(hours=1)),
        Aggregation(column='clicked', function='count', time_window=timedelta(hours=24)),
        Aggregation(column='clicked', function='count', time_window=timedelta(hours=72)),
    ],
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    description='The count of ad clicks for a user'
)
