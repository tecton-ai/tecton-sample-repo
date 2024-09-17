from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Int32

from fraud.entities import merchant
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[transactions_batch],
    entities=[merchant],
    mode='spark_sql',
    online=True,
    offline=True,
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('is_fraud', Int32), function='mean', time_window=timedelta(days=1)),
        Aggregate(input_column=Field('is_fraud', Int32), function='mean', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('is_fraud', Int32), function='mean', time_window=timedelta(days=90)),
    ],
    feature_start_time=datetime(2022, 5, 1),
    description='The merchant fraud rate over series of time windows, updated daily.',
    timestamp_field='timestamp'
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
