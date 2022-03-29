from tecton import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import DoubleType, StructType, StructField, LongType

request_schema = StructType([
    StructField('amount', DoubleType())
])
transaction_request = RequestDataSource(request_schema=request_schema)

output_schema = StructType([
    StructField('transaction_amount_is_high', LongType())
])


# This On-Demand Feature View evaluates a transaction amount and declares it as "high", if it's higher than 10,000
@on_demand_feature_view(
    inputs={'transaction_request': Input(transaction_request)},
    mode='python',
    output_schema=output_schema,
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production', 'prevent-destroy': 'true', 'prevent-recreate': 'true'},
    description='Whether the transaction amount is considered high (over $10000)'
)
def transaction_amount_is_high(transaction_request):

    result = {}
    result['transaction_amount_is_high'] = int(transaction_request['amount'] >= 10000)
    return result
