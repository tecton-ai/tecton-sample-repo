from tecton import batch_feature_view
from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from datetime import datetime, timedelta
from configs import dataproc_config


@batch_feature_view(
    sources=[fraud_users_batch],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=False,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017,1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='User date of birth, entered at signup.',
    timestamp_field='signup_timestamp',
    batch_compute=dataproc_config,
)
def user_home_location(fraud_users_batch):
    return f'''
        SELECT
            signup_timestamp,
            user_id,
            lat,
            long
        FROM
            {fraud_users_batch}
    '''
