from tecton.compat import batch_feature_view, Input, BackfillConfig
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

@batch_feature_view(
    inputs={'transactions': Input(transactions_batch)},
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 5, 20),
    batch_schedule='1d',
    ttl='30days',
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    family='fraud',
    description='Last user transaction amount (batch calculated)'
)
def last_transaction_amount(transactions):
    return f'''
        SELECT
            timestamp,
            nameorig as user_id,
            amount
        FROM
            {transactions}
        '''

# Note: this is an anti-example
# This technically works but will create unexpected results because the backfill time range isn't limited
# @batch_feature_view(
#     inputs={'transactions': Input(transactions_batch)},
#     entities=[user],
#     mode='spark_sql',
#     online=False,
#     offline=False,
#     feature_start_time=datetime(2021, 5, 20),
#     batch_schedule='1d',
#     ttl='30days',
#     backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
#     family='fraud',
#     description='Max user transaction amount'
# )
# def max_transaction_amount(transactions):
#     return f'''
#         SELECT
#             last(timestamp) as timestamp,
#             nameorig as user_id,
#             max(amount) as amount
#         FROM
#             {transactions}
#         GROUP BY nameorig
#         '''
