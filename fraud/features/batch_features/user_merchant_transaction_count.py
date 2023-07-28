from tecton import batch_feature_view, Aggregation, FilteredSource
from fraud.entities import user, merchant
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta
from configs import dataproc_config


@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user, merchant],
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
    owner='matt@tecton.ai',
    description='User transaction counts at specific merchants over a series of time windows, updated daily.',
    batch_compute=dataproc_config,
)
def user_merchant_transaction_counts(transactions):
    return f'''
        SELECT
            user_id,
            merchant,
            1 as transaction,
            timestamp
        FROM
            {transactions}
        '''
