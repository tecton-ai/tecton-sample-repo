from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(transaction_batch, start_time_offset=timedelta(days=29))],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    incremental_backfills=True,
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=1),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='How many transactions the user has made to distinct merchants in the last 30 days.'
)
def user_distinct_merchant_transaction_count_30d(transactions_batch):
    return f'''
        SELECT
            nameorig AS user_id,
            MAX(timestamp) as timestamp,
            COUNT(DISTINCT namedest) AS distinct_merchant_transaction_count_30d,
        FROM {transactions_batch}
        GROUP BY
            nameorig
    '''
