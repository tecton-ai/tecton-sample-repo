# from tecton import batch_feature_view, Input, materialization_context
# from fraud.entities import user
# from fraud.data_sources.transactions_batch import transactions_batch
# from datetime import datetime
#
#
# @batch_feature_view(
#     inputs={'transactions_batch': Input(transactions_batch, window='30d')},
#     entities=[user],
#     mode='spark_sql',
#     ttl='1d',
#     batch_schedule='1d',
#     online=True,
#     offline=True,
#     feature_start_time=datetime(2021, 4, 1),
#     family='fraud',
#     tags={'release': 'production'},
#     owner='matt@tecton.ai',
#     description='How many transactions the user has made to distinct merchants in the last 30 days.'
# )
# def user_distinct_merchant_transaction_count_30d(transactions_batch, context=materialization_context()):
#     return f'''
#         SELECT
#             name_orig as user_id,
#             count(distinct name_dest) as distinct_merchant_transaction_count_30d,
#             window.end - INTERVAL 1 SECOND as timestamp
#         FROM
#             {transactions_batch}
#         GROUP BY
#             user_id, window(timestamp, '30 days', '1 day')
#         HAVING
#             timestamp >= '{context.feature_start_time_string}' AND timestamp < '{context.feature_end_time_string}'
#         '''
