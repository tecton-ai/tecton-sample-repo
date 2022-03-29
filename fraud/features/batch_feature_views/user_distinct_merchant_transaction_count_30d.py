from tecton import batch_feature_view, Input, tecton_sliding_window, transformation, const, BackfillConfig, MonitoringConfig
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

# Counts distinct merchant names for each user and window. The timestamp
# for the feature is the end of the window.
# window_input_df is created by passing the original input through
# tecton_sliding_window transformation.
@transformation(mode='spark_sql')
def user_distinct_merchant_transaction_count_transformation(window_input_df):
    return f'''
        SELECT
            nameorig AS user_id,
            COUNT(DISTINCT namedest) AS distinct_merchant_count,
            window_end AS timestamp
        FROM {window_input_df}
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
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    monitoring=MonitoringConfig(alert_email="derek@tecton.ai", monitor_freshness=True),
    family='fraud',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='How many transactions the user has made to distinct merchants in the last 30 days.'
)
def user_distinct_merchant_transaction_count_30d(transactions_batch):
    return user_distinct_merchant_transaction_count_transformation(
        # Use tecton_sliding_transformation to create trailing 30 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        tecton_sliding_window(transactions_batch,
            timestamp_key=const('timestamp'),
            window_size=const('30d')))
