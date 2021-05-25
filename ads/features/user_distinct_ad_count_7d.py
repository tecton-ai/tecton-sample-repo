from tecton import batch_feature_view, Input, NewDatabricksClusterConfig
from core.entities import user
from ads.data_sources.ad_impressions_batch import ad_impressions_batch
from datetime import datetime

@batch_feature_view(
    inputs={'ad_impressions': Input(ad_impressions_batch, window='7d')},
    entities=[user],
    mode='spark_sql',
    ttl='1d',
    batch_schedule='1d',
    batch_materialization=NewDatabricksClusterConfig(
        instance_type='m5.xlarge',
        number_of_workers=4
    ),
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    family='ad_serving',
    tags={'release': 'production'},
    owner='david@tecton.ai',
    description='How many distinct advertisments a user has been shown in the last week'
)
def user_distinct_ad_count_7d(ad_impressions):
    return f'''
        SELECT
            user_uuid as user_id,
            count(distinct ad_id) as distinct_ad_count,
            window(timestamp, '7 days', '1 day').end as timestamp
        FROM
            {ad_impressions}
        GROUP BY user_uuid, window(timestamp, '7 days', '1 day')
        '''
