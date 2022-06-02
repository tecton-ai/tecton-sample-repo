# from tecton import RequestDataSource, on_demand_feature_view, Input
# from pyspark.sql.types import BooleanType, DoubleType, StructType, StructField
# from features.batch_feature_views.user_transaction_metrics import user_transaction_metrics
# from features.batch_feature_views.user_category_count import user_category_count
#
# # On-Demand Feature Views require enabling Snowpark.
# # Contact Tecton for assistance in enabling this feature.
#
#
# output_schema = StructType([
#     StructField('user_category_pct', DoubleType())
# ])
#
# @on_demand_feature_view(
#     inputs={
#         'user_transaction_metrics': Input(user_transaction_metrics),
#         'user_category_count': Input(user_category_count)
#     },
#     mode='python',
#     output_schema=output_schema,
#     description='Percent of a users transcations that have been in this category, in the last 40 days'
# )
# def user_category_pct(user_transaction_metrics, user_category_count):
#     transaction_count_40d = user_transaction_metrics['TRANSACTION_SUM_960H_1D']
#     category_count_40d = user_category_count['TRANSACTION_SUM_960H_1D']
#     if transaction_count_40d is None or transaction_count_40d == 0:
#         return {'user_category_pct': 0}
#     return {'user_category_pct': float(category_count_40d) / float(transaction_count_40d)}
