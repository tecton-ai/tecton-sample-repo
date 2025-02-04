from tecton import batch_feature_view, Attribute
from tecton import TectonTimeConstant
from tecton.types import Int64
from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[ad_impressions_batch.select_range(start_time=TectonTimeConstant.MATERIALIZATION_START_TIME - timedelta(days=6), end_time=TectonTimeConstant.MATERIALIZATION_END_TIME)],
    entities=[user],
    mode='spark_sql',
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
    return f'''
        SELECT
            user_uuid as user_id,
            approx_count_distinct(ad_id) as distinct_ad_count,
            TO_TIMESTAMP("{context.end_time}") - INTERVAL 1 MICROSECOND as timestamp
        FROM
            {ad_impressions}
        GROUP BY
            user_uuid
    '''
