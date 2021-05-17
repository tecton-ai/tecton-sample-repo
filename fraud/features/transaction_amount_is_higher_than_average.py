from tecton import RequestDataSource
from tecton.feature_views import on_demand_feature_view
from tecton.feature_views.feature_view import Input
from pyspark.sql.types import DoubleType, StructType, StructField, LongType
from fraud.features.user_transaction_amount_metrics import user_transaction_amount_metrics
import pandas


request_schema = StructType()
request_schema.add(StructField("amount", DoubleType()))
transaction_request = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField("transaction_amount_is_higher_than_average", LongType()))


@on_demand_feature_view(
    inputs={
        "transaction_request": Input(transaction_request),
        "user_transaction_amount_metrics": Input(user_transaction_amount_metrics)
    },
    mode="pandas",
    output_schema=output_schema,
    family="fraud",
    owner="matt@tecton.ai",
    tags={"release": "production"},
    description="The transaction amount is higher than the 1 day average."
)
def transaction_amount_is_higher_than_average(transaction_request: pandas.DataFrame, user_transaction_amount_metrics: pandas.DataFrame):
    import pandas as pd

    user_transaction_amount_metrics['amount_mean_24h_1h'] = user_transaction_amount_metrics['amount_mean_24h_1h'].fillna(0)

    df = pd.DataFrame()
    df['transaction_amount_is_higher_than_average'] = (transaction_request['amount'] > user_transaction_amount_metrics['amount_mean_24h_1h']).astype('int64')
    return df
