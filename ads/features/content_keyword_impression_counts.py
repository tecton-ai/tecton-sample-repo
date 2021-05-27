from tecton.feature_views import stream_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input
from tecton import FeatureAggregation
from ads.entities import content_keyword
from ads.data_sources.ad_impressions_stream import ad_impressions_stream
from datetime import datetime


@stream_window_aggregate_feature_view(
    inputs={'ad_impressions': Input(ad_impressions_stream)},
    entities=[content_keyword],
    mode='spark_sql',
    aggregation_slide_period='continuous', # enable low latency streaming
    aggregations=[FeatureAggregation(column='impression', function='count', time_windows=['1h', '12h', '24h','72h','168h'])],
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    family='ads',
    tags={'release': 'production'},
    owner='ross@tecton.ai',
    description='The count of ad impressions for a content_keyword'
)
def content_keyword_impression_counts(ad_impressions):
    return f'''
        SELECT
            content_keyword,
            1 as impression,
            timestamp
        FROM
            {ad_impressions}
        '''
