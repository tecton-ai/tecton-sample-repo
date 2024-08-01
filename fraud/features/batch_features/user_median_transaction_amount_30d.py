from tecton import batch_feature_view, Aggregation, FilteredSource, Aggregate
from tecton.aggregation_functions import approx_percentile
from tecton.types import Field, Float64

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('amt', Float64), function=approx_percentile(percentile=0.5), time_window=timedelta(days=30))
    ],
    description='Median transaction amount for a user over the last 30 days',
    timestamp_field='timestamp'
)
def user_median_transaction_amount_30d(transactions_batch):
    return f'''
        SELECT
            timestamp,
            user_id,
            amt
        FROM
            {transactions_batch}
        '''
