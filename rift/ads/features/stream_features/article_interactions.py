from tecton import stream_feature_view, Aggregate, AggregationLeadingEdge
from tecton.types import Int64, Field
from ads.entities import content, user
from ads.data_sources.article_interactions import article_interactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=article_interactions_stream,
    entities=[user, content],
    mode='pandas',
    features=[
        Aggregate(input_column=Field('interaction', Int64), function='count', time_window=timedelta(hours=1)),
        Aggregate(input_column=Field('interaction', Int64), function='count', time_window=timedelta(hours=24)),
        Aggregate(input_column=Field('interaction', Int64), function='count', time_window=timedelta(hours=72)),
    ],
    timestamp_field='timestamp',
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of interactions between a given user and a given article',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME,
    environment='tecton-core-1.1.0'
)
def user_article_interactions(interactions):
    df = interactions[['user_uuid', 'article_id', 'timestamp']].copy()
    
    # Rename user_uuid to user_id
    df = df.rename(columns={'user_uuid': 'user_id'})
    
    # Add interaction column with constant value 1
    df['interaction'] = 1
    
    return df 