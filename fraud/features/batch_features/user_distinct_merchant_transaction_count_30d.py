from tecton import batch_feature_view, FilteredSource, materialization_context
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# This feature view is an example of a "custom aggregation" using `incremental_backfills=True`. This allows users to
# compute aggregations over long windows of data using SQL aggregations.
#
# See this documentation for more info:
# https://docs.tecton.ai/latest/overviews/framework/feature_views/batch/incremental_backfills.html.
@batch_feature_view(
    sources=[FilteredSource(transactions_batch, start_time_offset=timedelta(days=-29))],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 4, 1),
    incremental_backfills=True,
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=2),
    owner='david@tecton.ai',
    tags={'release': 'production'},
    description='How many transactions the user has made to distinct merchants in the last 30 days.'
)
def user_distinct_merchant_transaction_count_30d(transactions_batch, context=materialization_context()):
    return f'''
        SELECT
            user_id,
            -- The timestamp for the feature data must be in the materialization range: [start_time, end_time). The
            -- best practice for "custom aggregations" to avoid data leakage is to use the latest timestamp in that
            -- range, i.e. end_time - 1 microsecond.
            TO_TIMESTAMP("{context.end_time}") - INTERVAL 1 MICROSECOND as timestamp,
            COUNT(DISTINCT merchant) AS distinct_merchant_transaction_count_30d
        FROM {transactions_batch}
        GROUP BY
            user_id
    '''
