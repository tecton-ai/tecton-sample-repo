from tecton import batch_feature_view, Input, materialization_context, transformation, const
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

@transformation(mode='pyspark')
def is_weekend(input_df, timestamp_column):
    from pyspark.sql.functions import dayofweek, col, to_timestamp
    return input_df.withColumn("is_weekend", dayofweek(to_timestamp(col(timestamp_column))).isin([1,7]).cast("int"))

@transformation(mode='spark_sql')
def weekend_transaction_count_n_days(input_df, context, count):
    return f'''
        SELECT
            nameorig as user_id,
            sum(is_weekend) as weekend_transaction_count_{count}d,
            window.end - INTERVAL 1 SECOND as timestamp
        FROM
            {input_df}
        GROUP BY
            user_id, window(timestamp, '{count} days', '1 day')
        HAVING
            timestamp >= '{context.feature_start_time_string}' AND timestamp < '{context.feature_end_time_string}'
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
def user_weekend_transaction_count_30d(transactions_batch, context=materialization_context()):
    return weekend_transaction_count_n_days(is_weekend(transactions_batch, const("timestamp")), context, const(30))
