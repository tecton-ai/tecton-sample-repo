import pandas
from tecton import transformation, on_demand_feature_view, Input, RequestDataSource
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Your spine dataframe or incoming request must include these features
request_schema = StructType([
    StructField('amount', LongType()),
    StructField('type_CASH_IN', LongType()),
    StructField('type_CASH_OUT', LongType()),
    StructField('type_DEBIT', LongType()),
    StructField('type_PAYMENT', LongType()),
    StructField('type_TRANSFER', LongType())
])

# These are the columns that will be returned by this feature view
output_schema = StructType([
    StructField('amount', LongType()),
    StructField('type', StringType())
])

# Bucket the transaction amount by each $10,000 dollars
# This is a reusable transformation you can use in multiple feature views
@transformation(mode="pandas")
def tx_amount_bucketed_transformation(transaction_request: pandas.DataFrame):
    import pandas as pd

    response = pd.DataFrame()
    response['amount'] = (transaction_request.amount / 10000).round().astype('long')
    return response

# Bucket the transaction type as credit, debit, or transfer
# This is a reusable transformation you can use in multiple feature views
@transformation(mode="pandas")
def tx_type_bucketed_transformation(transaction_request: pandas.DataFrame):
    response = transaction_request

    response['type'] = 'unknown'
    response.loc[response.type_CASH_IN > 0, 'type'] = 'credit'
    response.loc[response.type_CASH_OUT + response.type_DEBIT + response.type_PAYMENT > 0, 'type'] = 'debit'
    response.loc[response.type_TRANSFER > 0, 'type'] = 'transfer'
    return response[['type']]

# We combine the results of our two other transformers (type and amount bucketed) here in
# a third transformer
@transformation(mode='pandas')
def merge_dfs(df1: pandas.DataFrame, df2: pandas.DataFrame):
    import pandas as pd
    return pd.concat([df1, df2], axis=1)

# This on-demand feature view runs two different transformations in pipeline mode
# and returns features drawn from both transformers: amount_bucketed and type_bucketed
@on_demand_feature_view(
    description="[Online Feature] Bucket transaction amount and type",
    inputs={"transaction_request": Input(RequestDataSource(request_schema))},
    output_schema=output_schema,
    mode='pipeline',
    family='fraud',
    owner='jack@tecton.ai',
    tags={'release': 'production'}
)
def transaction_bucketing(transaction_request: pandas.DataFrame):
    # We merge the results of the two transformations in a third transformation
    return merge_dfs(tx_amount_bucketed_transformation(transaction_request),
                     tx_type_bucketed_transformation(transaction_request))
