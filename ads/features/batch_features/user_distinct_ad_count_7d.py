from tecton import batch_feature_view, FilteredSource, materialization_context
from ads.entities import user
from ads.data_sources.ad_impressions import ad_impressions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(ad_impressions_batch, start_time_offset=timedelta(days=-6))],
    entities=[user],
    mode='spark_sql',
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    incremental_backfills=True,
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production', 'usecase': 'ads'},
    owner='david@tecton.ai',
    description='How many distinct advertisements a user has been shown in the last week'
)
def user_distinct_ad_count_7d(ad_impressions, context=materialization_context()):
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
