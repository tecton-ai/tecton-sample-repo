from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.users import users
from data_sources.transactions_batch import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(users), FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule=timedelta(days=1),
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Whether the user performing the transaction is over 18 years old.'
)
def transaction_user_is_adult(users, transactions_batch):
    return f'''
        select
            user_id,
            timestamp,
            IF (datediff(timestamp, dob) > (18*365), 1, 0) as user_is_adult
        from {transactions} t
        join {users} u on t.nameorig=u.user_id
    '''
