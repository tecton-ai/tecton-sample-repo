from tecton.v09_compat import stream_feature_view, FilteredSource, Aggregation
from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(ad_impressions_stream),
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(hours=1),
    aggregations=[
        Aggregation(column='clicked', function='count', time_window=timedelta(hours=1)),
        Aggregation(column='clicked', function='count', time_window=timedelta(hours=24)),
        Aggregation(column='clicked', function='count', time_window=timedelta(hours=72)),
    ],
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of ad clicks for a user'
)
def user_click_counts(ad_impressions):
    return ad_impressions.select(ad_impressions['user_uuid'].alias('user_id'), 'clicked', 'timestamp')
