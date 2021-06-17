from tecton import batch_feature_view, Input, materialization_contexts, DatabricksClusterConfig
from ads.entities import user
from ads.data_sources.ad_impressions_batch import ad_impressions_batch
from datetime import datetime

@batch_feature_view(
    inputs={'ad_impressions': Input(ad_impressions_batch, window='7d')},
    entities=[user],
    mode='spark_sql',
    ttl='1d',
    batch_schedule='1d',
    online=True,
    offline=True,
    batch_cluster_config=DatabricksClusterConfig(
        instance_type='m5.xlarge',
        number_of_workers=4,
        spark_config={
            'spark.executor.memory': '5000m',
        },
    ),
    feature_start_time=datetime(2021, 4, 1),
    family='ad_serving',
    tags={'release': 'production'},
    owner='david@tecton.ai',
    description='How many distinct advertisments a user has been shown in the last week'
)
def user_distinct_ad_count_7d(ad_impressions, context=materialization_context()):
    return f'''
        SELECT
            user_uuid as user_id,
            count(distinct ad_id) as distinct_ad_count,
            window.end - INTERVAL 1 SECOND as timestamp
        FROM
            {ad_impressions}
        GROUP BY
            user_uuid, window(timestamp, '7 days', '1 day')
        HAVING
            timestamp >= '{context.feature_start_time_string}' AND timestamp < '{context.feature_end_time_string}'
        '''
