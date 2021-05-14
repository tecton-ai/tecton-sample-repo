from tecton import RequestDataSource
from tecton.feature_views import on_demand_feature_view
from tecton.feature_views.feature_view import Input
from pyspark.sql.types import DoubleType, StructType, StructField, LongType
import pandas

request_schema = StructType()
request_schema.add(StructField("amount", DoubleType()))
transaction_request = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField("transaction_amount_is_high", LongType()))

@on_demand_feature_view(
    inputs={"transaction_request": Input(transaction_request)},
    mode="pandas",
    output_schema=output_schema,
    family="fraud",
    owner="matt@tecton.ai",
    tags={"release": "production"},
    description="Whether the transaction amount is considered high (over $10000)"
)
def transaction_amount_is_high(transaction_request: pandas.DataFrame):
    import pandas as pd
    df = pd.DataFrame()
    df['transaction_amount_is_high'] = (transaction_request['amount'] >= 10000).astype('int64')
    return df
