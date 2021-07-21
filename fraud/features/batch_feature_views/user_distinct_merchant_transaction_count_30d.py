from tecton import batch_feature_view, Input, tecton_sliding_window, transformation, const
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

# This transformation input is the output of the tecton_sliding_window
# transformation, which appends the window_end timestamp as the end of the
# aggregation period.
@transformation(mode='spark_sql')
def user_distinct_merchant_transaction_count_transformation(windowed_input_df):
    return f'''
        SELECT
            nameorig AS user_id,
            COUNT(DISTINCT namedest) AS distinct_merchan_count,
            window_end AS timestamp
        FROM {windowed_input_df}
        GROUP BY
            nameorig,
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
    description='How many transactions the user has made to distinct merchants in the last 30 days.'
)
def user_distinct_merchant_transaction_count_30d(transactions_batch):
    # Use the sliding_window_transformation to create trailing 30 day time windows.
    # The slide_interval defaults to the batch_schedule (1 day).
    return user_distinct_merchant_transaction_count_transformation(
        tecton_sliding_window(transactions_batch, timestamp_col=const('timestamp'), window_size=const('30d')))
