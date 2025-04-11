from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Int64, Timestamp
from datetime import datetime, timedelta

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch

@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode='pandas',
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(
            input_column=Field('weekend_transaction_count_30d', Int64),
            function='sum',
            time_window=timedelta(days=30)
        )
    ],
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    tags={'cost-center': 'finance'},
    description='How many weekend transactions the user has made in the last 30 days.',
    timestamp_field='timestamp'
)
def user_weekend_transaction_count_30d(transactions_batch):
    import pandas as pd
    
    df = transactions_batch[['user_id', 'timestamp']].copy()
    
    # Create a column for weekend transactions (1 if weekend, 0 if not)
    df['weekend_transaction_count_30d'] = df['timestamp'].dt.dayofweek.isin([5, 6]).astype(int)
    
    result = df.groupby('user_id').agg({
        'weekend_transaction_count_30d': 'sum',
        'timestamp': 'max'
    }).reset_index()
    
    result['timestamp'] = pd.to_datetime(result['timestamp']).dt.tz_convert('UTC').astype('datetime64[us, UTC]')
    
    return result
