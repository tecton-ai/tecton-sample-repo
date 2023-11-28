from tecton import batch_feature_view, FilteredSource, Aggregation
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='transaction', function='count', time_window=timedelta(days=1)),
        Aggregation(column='transaction', function='count', time_window=timedelta(days=30)),
        Aggregation(column='transaction', function='count', time_window=timedelta(days=90))
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User transaction totals over a series of time windows, updated daily.'
)
def user_transaction_counts(transactions):
    return f'''
        SELECT
            user_id,
            1 as transaction,
            timestamp
        FROM
            {transactions}
        '''
