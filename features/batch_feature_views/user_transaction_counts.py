from tecton.feature_views import batch_feature_view, FilteredSource, Aggregation
from entities import user
from data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='User transaction totals over a series of time windows, updated daily.',
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='transaction', function='count', time_windows=[timedelta(days=1), timedelta(days=3), timedelta(days=7)])
    ]
)
def user_transaction_counts(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            1 as transaction,
            timestamp
        FROM
            {transactions}
        '''
