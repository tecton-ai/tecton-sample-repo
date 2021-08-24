from tecton import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import DoubleType, StructType, StructField, LongType
import pandas


request_schema = StructType()
request_schema.add(StructField('amount', DoubleType()))
transaction_request = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField('transaction_amount_is_high', LongType()))


# This On-Demand Feature View evaluates a transaction amount and declares it as "high", if it's higher than 10,000
@on_demand_feature_view(
    inputs={'transaction_request': Input(transaction_request)},
    mode='pandas',
    output_schema=output_schema,
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production', 'prevent-destroy': 'true', 'prevent-recreate': 'true'},
    description='Whether the transaction amount is considered high (over $10000)'
)
def transaction_amount_is_high(transaction_request: pandas.DataFrame):
    import pandas as pd

    df = pd.DataFrame()
    df['transaction_amount_is_high'] = (transaction_request['amount'] >= 10000).astype('int64')
    return df
