from tecton import RequestDataSource, on_demand_feature_view, Input
from pyspark.sql.types import DoubleType, StructType, StructField, LongType
from fraud.features.user_transaction_amount_metrics import user_transaction_amount_metrics
import pandas

# Defining the schema of a transaction request which will be used as an input
request_schema = StructType()
request_schema.add(StructField("amount", DoubleType()))
transaction_request = RequestDataSource(request_schema=request_schema)

# Defining the schema of the transformed feature value(s)
output_schema = StructType()
output_schema.add(StructField("transaction_amount_is_higher_than_average", LongType()))

# This On-Demand Feature View compares request data ("amount")
# to a feature ("amount_mean_24h_1h") from a pre-computed Feature View ("user_transaction_amount_metrics").
@on_demand_feature_view(
    inputs={
        "transaction_request": Input(transaction_request), # the incoming transaction request defined by the request_schema above
        "user_transaction_amount_metrics": Input(user_transaction_amount_metrics) # an imported stream aggregation Feature View
    },
    mode="pandas", # On-Demand Feature Views always use the mode "pandas", which is used in the function below
    output_schema=output_schema,
    family="fraud",
    owner="matt@tecton.ai",
    tags={"release": "production"}, # Set any custom tags you want
    description="The transaction amount is higher than the 1 day average."
)
# The Feature View name defaults to this function name, but can be overridden with a `name_override` param in the decorator above.
# The parameters map to the `inputs` dict keys fromt he decorator above.
def transaction_amount_is_higher_than_average(transaction_request: pandas.DataFrame, user_transaction_amount_metrics: pandas.DataFrame):
    import pandas as pd

    # This column is a feature in the "user_transaction_amount_metrics" Feature View.
    # The feature values are null if there are no transactions in the 24h window so here we fill the nulls with 0.
    user_transaction_amount_metrics['amount_mean_24h_1h'] = user_transaction_amount_metrics['amount_mean_24h_1h'].fillna(0)

    # Constructing and returning the final dataframe of transformed feature values
    df = pd.DataFrame()
    df['transaction_amount_is_higher_than_average'] = (transaction_request['amount'] > user_transaction_amount_metrics['amount_mean_24h_1h']).astype('int64')
    return df
