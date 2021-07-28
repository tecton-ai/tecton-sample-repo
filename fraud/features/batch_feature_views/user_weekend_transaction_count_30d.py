from tecton import batch_feature_view, Input, transformation, const, tecton_sliding_window
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

@transformation(mode='pyspark')
def is_weekend(input_df, timestamp_column):
    from pyspark.sql.functions import dayofweek, col, to_timestamp
    return input_df.withColumn("is_weekend", dayofweek(to_timestamp(col(timestamp_column))).isin([1,7]).cast("int"))

# Counts distinct merchant names for each user and window. The timestamp
# for the feature is the end of the window.
# window_input_df is created by passing the original input through
# tecton_sliding_window transformation.
@transformation(mode='spark_sql')
def weekend_transaction_count_n_days(window_input_df, window_size):
    return f'''
        SELECT
            nameorig as user_id,
            sum(is_weekend) as weekend_transaction_count_{window_size},
            window_end AS timestamp
        FROM
            {window_input_df}
        GROUP BY
            user_id,
            window_end
        '''

@batch_feature_view(
    inputs={'transactions_batch': Input(transactions_batch, window='30d')},
    entities=[user],
    mode='pipeline',
    ttl='1d',
    batch_schedule='1d',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    family='fraud',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='How many weekend transactions the user has made in the last 30 days.'
)
def user_weekend_transaction_count_30d(transactions_batch):
    timestamp_key = const("timestamp")
    window_size = const("30d")
    return weekend_transaction_count_n_days(
        # Use tecton_sliding_transformation to create trailing 30 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        tecton_sliding_window(
            is_weekend(transactions_batch, timestamp_key),
            timestamp_key=timestamp_key,
            window_size=window_size),
        window_size=window_size)
