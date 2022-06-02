from tecton import batch_feature_view, Aggregation, FilteredSource
from fraud.entities import merchant
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[merchant],
    mode='spark_sql',
    online=True,
    offline=True,
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='is_fraud', function='mean', time_window=timedelta(days=1)),
        Aggregation(column='is_fraud', function='mean', time_window=timedelta(days=30)),
        Aggregation(column='is_fraud', function='mean', time_window=timedelta(days=90))
    ],
    feature_start_time=datetime(2022, 5, 1),
    description='The merchant fraud rate over series of time windows, updated daily.'
)
def merchant_fraud_rate(transactions_batch):
    return f'''
        SELECT
            merchant,
            is_fraud,
            timestamp
        FROM
            {transactions_batch}
        '''
