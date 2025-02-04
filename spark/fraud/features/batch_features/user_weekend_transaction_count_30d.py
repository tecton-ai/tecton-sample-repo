from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Int32

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta

def is_weekend(input_df, timestamp_column):
    from pyspark.sql.functions import dayofweek, col, to_timestamp
    return input_df.withColumn("is_weekend", dayofweek(to_timestamp(col(timestamp_column))).isin([1,7]).cast("int"))

@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('is_weekend', Int32), function='count', time_window=timedelta(days=30)),
    ],
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    tags={'cost-center': 'finance'},
    description='How many weekend transactions the user has made in the last 30 days.',
    timestamp_field='timestamp'
)
def user_weekend_transaction_count_30d(transactions_batch):
    return is_weekend(transactions_batch, "timestamp") \
        .select('user_id', 'is_weekend', 'timestamp')
