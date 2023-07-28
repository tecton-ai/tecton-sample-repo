from tecton import batch_feature_view
from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from datetime import datetime, timedelta
from configs import dataproc_config


@batch_feature_view(
    sources=[fraud_users_batch],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    timestamp_field='signup_timestamp',
    prevent_destroy=False,  # Set to True to prevent accidental destructive changes or downtime.
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='User credit card issuer derived from the user credit card number.',
    batch_compute=dataproc_config,
)
def user_credit_card_issuer(fraud_users_batch):
    return f'''
        SELECT
            user_id,
            signup_timestamp,
            CASE SUBSTRING(CAST(cc_num AS STRING), 0, 1)
                WHEN '4' THEN 'Visa'
                WHEN '5' THEN 'MasterCard'
                WHEN '6' THEN 'Discover'
                ELSE 'other'
            END as credit_card_issuer
        FROM
            {fraud_users_batch}
        '''
