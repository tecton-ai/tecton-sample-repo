from tecton import batch_feature_view, Input
from fraud.entities import user
from fraud.data_sources.credit_scores_batch import credit_scores_batch
from datetime import datetime


# @batch_feature_view(
#     inputs={'credit_scores': Input(credit_scores_batch)},
#     entities=[user],
#     mode='spark_sql',
#     online=True,
#     offline=True,
#     feature_start_time=datetime(2020, 10, 10),
#     batch_schedule='1d',
#     ttl='730days',
#     family='fraud',
#     description='Whether the user has a great credit score (over 740).'
# )
# def user_has_great_credit(credit_scores):
#     return f'''
#         SELECT
#             user_id,
#             IF (credit_score > 740, 1, 0) as user_has_great_credit,
#             date as timestamp
#         FROM
#             {credit_scores}
#         '''
