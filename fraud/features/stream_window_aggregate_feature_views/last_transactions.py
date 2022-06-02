# from tecton import stream_window_aggregate_feature_view, Input, FeatureAggregation
# from tecton.aggregation_functions import last_distinct
# from fraud.entities import user
# from fraud.data_sources.transactions_stream import transactions_stream
# from datetime import datetime
#
#
# # The following defines a sliding time window aggregation that collects the last N transaction amounts of a user
# @stream_window_aggregate_feature_view(
#     inputs={'transactions': Input(transactions_stream)},
#     entities=[user],
#     mode='spark_sql',
#     aggregation_slide_period='10m',  # Defines how frequently feature values get updated in the online store
#     batch_schedule='1d', # Defines how frequently batch jobs are scheduled to ingest into the offline store
#     aggregations=[
#         FeatureAggregation(column='amount', function=last_distinct(10),  time_windows=['1h', '12h', '24h','72h'])
#     ],
#     online=False,
#     offline=False,
#     feature_start_time=datetime(2020, 10, 10),
#     family='fraud',
#     tags={'release': 'production'},
#     owner='kevin@tecton.ai',
#     description='Most recent 10 transaction amounts of a user'
# )
# def user_recent_transactions(transactions):
#     return f'''
#         SELECT
#             nameorig as user_id,
#             cast(amount as string) as amount,
#             timestamp
#         FROM
#             {transactions}
#         '''
