from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Float64
from tecton.aggregation_functions import approx_percentile
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode='pandas',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    aggregation_interval=timedelta(days=1),
    environment='tecton-core-1.1.0',
    description='Median transaction amount for a user over the last 30 days',
    features=[
        Aggregate(input_column=Field('amt', Float64), function=approx_percentile(0.5), time_window=timedelta(days=30))
    ],
    timestamp_field='timestamp',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)
def user_median_transaction_amount_30d(transactions_batch):
    df = transactions_batch.copy()
    df = df.sort_values(['user_id', 'timestamp'])
    
    # Calculate rolling medians for each user
    df['amt'] = df.groupby('user_id')['amt'].transform(lambda x: x.expanding().median())
    
    return df[['timestamp', 'user_id', 'amt']]
