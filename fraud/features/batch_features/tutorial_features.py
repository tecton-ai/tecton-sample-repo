from tecton import batch_feature_view, Aggregation, FilteredSource
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# @batch_feature_view(
#     sources=[transactions_batch],
#     entities=[user],
#     mode='spark_sql',
#     online=True,
#     batch_schedule=timedelta(days=1),
#     ttl=timedelta(days=30),
#     feature_start_time=datetime(2022, 5, 1),
#     description='Last user transaction amount (batch calculated)'
# )
# def user_last_transaction_amount(transactions):
#     return f'''
#         SELECT
#             user_id,
#             amt,
#             timestamp
#         FROM
#             {transactions}
#         '''
#
#
# @batch_feature_view(
#     sources=[transactions_batch],
#     entities=[user],
#     mode='spark_sql',
#     online=True,
#     feature_start_time=datetime(2022, 5, 1),
#     description='Max transaction amounts for the user in various time windows',
#     aggregation_interval=timedelta(days=1),
#     aggregations=[Aggregation(column='amt', function='max', time_window=timedelta(days=7))],
# )
# def user_max_transactions(transactions):
#     return f'''
#         SELECT
#             user_id,
#             amt,
#             timestamp
#         FROM
#             {transactions}
#         '''
