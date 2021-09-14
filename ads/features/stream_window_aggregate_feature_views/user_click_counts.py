from tecton import stream_window_aggregate_feature_view
from tecton import Input
from tecton import FeatureAggregation
from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime


@stream_window_aggregate_feature_view(
    inputs={'ad_impressions': Input(ad_impressions_stream)},
    entities=[user],
    mode='pyspark',
    aggregation_slide_period='1h',
    aggregations=[FeatureAggregation(column='clicked', function='sum', time_windows=['1h', '12h', '24h','72h','168h'])],
    online=False,
    offline=False,
    batch_schedule='1d',
    feature_start_time=datetime(2021, 1, 1),
    family='ads',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='The count of ad clicks for a user'
)
def user_click_counts(ad_impressions):
    return ad_impressions.select(ad_impressions['user_uuid'].alias('user_id'), 'clicked', 'timestamp')
