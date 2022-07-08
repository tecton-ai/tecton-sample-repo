from tecton.compat import batch_window_aggregate_feature_view
from tecton.compat import FeatureAggregation
from tecton.compat import Input
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

def is_weekend(input_df, timestamp_column):
    from pyspark.sql.functions import dayofweek, col, to_timestamp
    return input_df.withColumn("is_weekend", dayofweek(to_timestamp(col(timestamp_column))).isin([1,7]).cast("int"))

@batch_window_aggregate_feature_view(
    inputs={'transactions_batch': Input(transactions_batch)},
    entities=[user],
    mode='pyspark',
    aggregation_slide_period='1d',
    aggregations=[FeatureAggregation(column='is_weekend', function='sum', time_windows=['30d'])],
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 4, 1),
    tags={'cost-center': 'finance'},
    description='How many weekend transactions the user has made in the last 30 days.'
)
def user_weekend_transaction_count_30d(transactions_batch):
    return is_weekend(transactions_batch, "timestamp") \
        .withColumnRenamed("nameorig", "user_id") \
        .select('user_id', 'is_weekend', 'timestamp')
