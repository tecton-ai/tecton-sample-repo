from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Int32

from fraud.entities import user, merchant
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[transactions_batch],
    entities=[user, merchant],
    mode='pandas',
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('count', Int32), function='count', time_window=timedelta(days=1)),
        Aggregate(input_column=Field('count', Int32), function='count', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('count', Int32), function='count', time_window=timedelta(days=90)),
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User transaction counts at specific merchants over a series of time windows, updated daily.',
    timestamp_field='timestamp',
    environment='tecton-core-1.1.0'
)
def user_merchant_transaction_count(transactions_batch):
    pandas_df = transactions_batch[['user_id', 'merchant', 'timestamp']].copy()
    pandas_df['count'] = 1
    return pandas_df
