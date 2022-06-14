from tecton import stream_feature_view, FilteredSource, Aggregation
from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(ad_impressions_stream),
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=1),
    aggregations=[
        Aggregation(column='impression', function='count', time_window=timedelta(hours=1)),
        Aggregation(column='impression', function='count', time_window=timedelta(hours=24)),
        Aggregation(column='impression', function='count', time_window=timedelta(hours=72)),
    ],
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='The count of ad impressions for a user'
)
def user_impression_counts(ad_impressions):
    return f'''
        SELECT
            user_uuid as user_id,
            1 as impression,
            timestamp
        FROM
            {ad_impressions}
        '''
