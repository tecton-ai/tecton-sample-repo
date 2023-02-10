from datetime import timedelta, datetime
from tecton import StreamFeatureView, Aggregation
from ads.entities import user
from ads.data_sources.ad_impressions import user_click_source

# Sample StreamFeatureView with PushSource and no batch
user_click_counts_wafv = StreamFeatureView(
    name="user_click_counts_wafv",
    source=user_click_source,
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