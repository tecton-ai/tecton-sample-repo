from tecton import batch_feature_view, Input, BackfillConfig
from fraud.entities import user
from fraud.data_sources.fraud_users_batch import fraud_users_batch
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime


# For every transaction, the following FeatureView precomputes a feature that indicates
# whether a user was an adult as of the time of the transaction
@batch_feature_view(
    inputs={'users': Input(fraud_users_batch, window="unbounded_preceding"),
            'transactions': Input(transactions_batch)},
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule='1d',
    ttl='120d',
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    family='fraud',
    tags={'release': 'production'},
    owner='kevin@tecton.ai',
    description='Whether the user had a good credit score (over 670) as of the time of a transaction.'
)
def transaction_user_is_adult(users, transactions):
    return f'''
        select 
          timestamp, 
          user_id,  
          IF (datediff(timestamp, dob) > (18*365), 1, 0) as user_is_adult
          
          from {transactions} t
          join {users} u on t.nameorig=u.user_id
    '''
