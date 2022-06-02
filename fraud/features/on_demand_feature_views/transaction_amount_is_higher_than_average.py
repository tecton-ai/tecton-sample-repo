# from tecton import RequestDataSource, on_demand_feature_view
# from pyspark.sql.types import BooleanType, DoubleType, StructType, StructField
# from features.batch_feature_views.user_transaction_metrics import user_transaction_metrics

# On-Demand Feature Views require enabling Snowpark.
# Contact Tecton for assistance in enabling this feature.


# request_schema = StructType([
#     StructField('AMT', DoubleType())
# ])
# transaction_request = RequestDataSource(request_schema=request_schema)

# output_schema = StructType([
#     StructField('transaction_amount_is_higher_than_average', BooleanType())
# ])

# @on_demand_feature_view(
#     inputs={
#         'transaction_request': Input(transaction_request),
#         'user_transaction_metrics': Input(user_transaction_metrics)
#     },
#     mode='python',
#     output_schema=output_schema,
#     description='The transaction amount is higher than the 1 day average.'
# )
# def transaction_amount_is_higher_than_average(transaction_request, user_transaction_metrics):
#     amount_mean = 0 if user_transaction_metrics['AMT_MEAN_24H_1D'] == None else user_transaction_metrics['AMT_MEAN_24H_1D']
#     return {'transaction_amount_is_higher_than_average': transaction_request['AMT'] > amount_mean}
