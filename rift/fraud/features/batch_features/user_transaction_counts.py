from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Int32

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode='pandas',
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(days=1)),
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(days=90)),
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User transaction totals over a series of time windows, updated daily.',
    timestamp_field='timestamp'
)
def user_transaction_counts(transactions):
    df = transactions[['user_id', 'timestamp']].copy()
    
    # Add transaction column with constant value 1
    df['transaction'] = 1
    
    return df
