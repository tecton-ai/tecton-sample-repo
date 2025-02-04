from tecton import stream_feature_view, Aggregate, AggregationLeadingEdge
from tecton.types import Field, Int64

from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=ad_impressions_stream,
    entities=[user],
    mode='pandas',
    features=[
        Aggregate(input_column=Field('clicked', Int64), function='count', time_window=timedelta(hours=1)),
        Aggregate(input_column=Field('clicked', Int64), function='count', time_window=timedelta(hours=24)),
        Aggregate(input_column=Field('clicked', Int64), function='count', time_window=timedelta(hours=72)),
    ],
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of ad clicks for a user',
    timestamp_field='timestamp',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME
)
def user_click_counts(ad_impressions):
    df = ad_impressions[['user_uuid', 'clicked', 'timestamp']]
    return df.rename(columns={'user_uuid': 'user_id'})
