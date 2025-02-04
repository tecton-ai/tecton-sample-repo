from tecton import stream_feature_view, Aggregate, AggregationLeadingEdge
from tecton.types import Int32, Field

from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=ad_impressions_stream,
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=1),
    features=[
        Aggregate(input_column=Field('impression', Int32), function='count', time_window=timedelta(hours=1)),
        Aggregate(input_column=Field('impression', Int32), function='count', time_window=timedelta(hours=24)),
        Aggregate(input_column=Field('impression', Int32), function='count', time_window=timedelta(hours=72)),
    ],
    timestamp_field='timestamp',
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of ad impressions for a user',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME
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
