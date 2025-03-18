from tecton import stream_feature_view, StreamProcessingMode, Aggregate, \
    AggregationLeadingEdge
from tecton.types import Field, Int32

from ads.entities import content_keyword
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta

@stream_feature_view(
    source=ad_impressions_stream,
    entities=[content_keyword],
    mode='pandas',
    stream_processing_mode=StreamProcessingMode.CONTINUOUS, # enable low latency streaming
    features=[
        Aggregate(input_column=Field('clicked', Int32), function='count', time_window=timedelta(minutes=1)),
        Aggregate(input_column=Field('clicked', Int32), function='count', time_window=timedelta(minutes=5)),
    ],
    timestamp_field='timestamp',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of ad impressions for a content_keyword',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME
)
def content_keyword_click_counts(ad_impressions):
    df = ad_impressions[['content_keyword', 'clicked', 'timestamp']].copy()
    df['clicked'] = df['clicked'].astype(float).abs().astype(int)
    return df
