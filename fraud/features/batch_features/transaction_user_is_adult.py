from tecton import batch_feature_view, FilteredSource, Attribute
from tecton.types import Int32

from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# For every transaction, the following FeatureView precomputes a feature that indicates
# whether a user was an adult as of the time of the transaction
@batch_feature_view(
    sources=[FilteredSource(transactions_batch), fraud_users_batch],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=100),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Whether the user performing the transaction is over 18 years old.',
    features=[
        Attribute('user_is_adult', Int32)
    ],
    timestamp_field='timestamp'
)
def transaction_user_is_adult(transactions_batch, fraud_users_batch):
    return f'''
        select
          timestamp,
          t.user_id,
          IF (datediff(timestamp, to_date(dob)) > (18*365), 1, 0) as user_is_adult
        from {transactions_batch} t join {fraud_users_batch} u on t.user_id=u.user_id
    '''
