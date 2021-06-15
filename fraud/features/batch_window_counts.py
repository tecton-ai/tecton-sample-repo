from tecton import batch_feature_view, Input
from fraud.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

# Note: this is an anti-example
# This technically works but will create unexpected results because the backfill time range isn't limited
# @batch_feature_view(
#     inputs={'transactions': Input(transactions_batch)},
#     entities=[user],
#     mode='spark_sql',
#     online=True,
#     offline=True,
#     feature_start_time=datetime(2021, 5, 20),
#     batch_schedule='1d',
#     ttl='30days',
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

@batch_feature_view(
    inputs={'transactions': Input(transactions_batch)},
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 5, 20),
    batch_schedule='1d',
    ttl='30days',
    family='fraud',
    description='Last user transaction amount'
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

@batch_feature_view(
    inputs={'transactions': Input(transactions_batch, window="7d")},
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 5, 20),
    batch_schedule='1d',
    ttl='30days',
    family='fraud',
    description='Max user transaction amount'
)
def seven_days_transaction_amount(transactions):
    return f'''
        SELECT
            window(timestamp, '7 days', '1 day').end  - INTERVAL 1 MILLISECOND as timestamp,
            nameorig as user_id,
            sum(amount) as amount
        FROM
            {transactions}
        GROUP BY nameorig, window(timestamp, '7 days', '1 day')
        '''
