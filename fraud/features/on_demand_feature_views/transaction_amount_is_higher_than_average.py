from tecton import RequestDataSource, on_demand_feature_view, Input
from pyspark.sql.types import BooleanType, DoubleType, StructType, StructField
from fraud.features.stream_window_aggregate_feature_views.user_transaction_amount_metrics import user_transaction_amount_metrics
import pandas

# Schema of the input to the OnDemandFeatureView
request_schema = StructType([
    StructField('amount', DoubleType())
])
transaction_request = RequestDataSource(request_schema=request_schema)

# Schema of the output feature value(s)
output_schema = StructType([
    StructField('transaction_amount_is_higher_than_average', BooleanType())
])

# This On-Demand Feature View compares request data ('amount')
# to a feature ('amount_mean_24h') from a pre-computed Feature View ('user_transaction_amount_metrics').
@on_demand_feature_view(
    inputs={
        'transaction_request': Input(transaction_request),
        'user_transaction_amount_metrics': Input(user_transaction_amount_metrics)
    },
    mode='pandas',
    output_schema=output_schema,
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='The transaction amount is higher than the 1 day average.'
)
def transaction_amount_is_higher_than_average(transaction_request: pandas.DataFrame, user_transaction_amount_metrics: pandas.DataFrame):
    import pandas as pd

    # This column is a feature in the 'user_transaction_amount_metrics' Feature View.
    # The feature values are null if there are no transactions in the 24h window so here we fill the nulls with 0.
    user_transaction_amount_metrics['amount_mean_24h_10m'] = user_transaction_amount_metrics['amount_mean_24h_10m'].fillna(0)

    df = pd.DataFrame()
    df['transaction_amount_is_higher_than_average'] = transaction_request['amount'] > user_transaction_amount_metrics['amount_mean_24h_10m']
    return df
