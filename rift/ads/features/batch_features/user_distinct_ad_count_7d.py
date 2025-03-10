from tecton import batch_feature_view, Attribute
from tecton import TectonTimeConstant
from tecton.types import Int64
from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_batch
from datetime import datetime, timedelta
import pandas


@batch_feature_view(
    sources=[ad_impressions_batch.select_range(start_time=TectonTimeConstant.MATERIALIZATION_START_TIME - timedelta(days=6), end_time=TectonTimeConstant.MATERIALIZATION_END_TIME)],
    entities=[user],
    mode='pandas',
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    incremental_backfills=True,
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production', 'usecase': 'ads'},
    owner='demo-user@tecton.ai',
    description='How many distinct advertisements a user has been shown in the last week',
    timestamp_field='timestamp',
    features=[
        Attribute(name='distinct_ad_count', dtype=Int64),
    ]
)
def user_distinct_ad_count_7d(ad_impressions, context):
    import pandas

    # Group by user_uuid and count distinct ad_ids
    df = ad_impressions.groupby('user_uuid').agg({
        'ad_id': 'nunique'
    }).reset_index()
    
    # Rename columns to match SQL output
    df.columns = ['user_id', 'distinct_ad_count']
    
    # Add timestamp column
    df['timestamp'] = pandas.Timestamp(context.end_time) - pandas.Timedelta(microseconds=1)
    
    return df
