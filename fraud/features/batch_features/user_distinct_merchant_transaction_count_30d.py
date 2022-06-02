from tecton import batch_feature_view, FilteredSource, materialization_context
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(transactions_batch, start_time_offset=timedelta(days=-29))],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
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
            TO_TIMESTAMP("{context.end_time}") - INTERVAL 1 MICROSECOND as timestamp,
            COUNT(DISTINCT merchant) AS distinct_merchant_transaction_count_30d
        FROM {transactions_batch}
        GROUP BY
            user_id
    '''
