from tecton import RequestDataSource
from tecton.feature_views import on_demand_feature_view
from tecton.feature_views.feature_view import Input
from pyspark.sql.types import StringType, StructType, StructField, LongType
from fraud.features.user_date_of_birth import user_date_of_birth
import pandas

request_schema = StructType()
request_schema.add(StructField("timestamp", StringType()))
transaction_request = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField("user_age", LongType()))

@on_demand_feature_view(
    inputs={
        "transaction_request": Input(transaction_request),
        "user_date_of_birth": Input(user_date_of_birth)
    },
    mode="pandas",
    output_schema=output_schema,
    family="fraud_detection",
    owner="matt@tecton.ai",
    tags={"release": "production"},
    description="The user's age in days."
)
def user_age(transaction_request: pandas.DataFrame, user_date_of_birth: pandas.DataFrame):
    import pandas as pd
    transaction_request['timestamp'] = pd.to_datetime(transaction_request['timestamp'])
    user_date_of_birth['user_date_of_birth'] = pd.to_datetime(user_date_of_birth['user_date_of_birth'])

    df = pd.DataFrame()
    df['user_age'] = (transaction_request['timestamp'] - user_date_of_birth['user_date_of_birth']).dt.days
    return df
