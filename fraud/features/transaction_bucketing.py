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
    StructField('amount_bucketed', LongType()),
    StructField('type_bucketed', StringType())
])

# Bucket the transaction amount by each $10,000 dollars
# This is a reusable transformation you can use in multiple feature views
@transformation(mode="pandas")
def tx_amount_bucketed_transformation(tx: pandas.DataFrame):
    import pandas as pd

    df = pd.DataFrame()
    df['amount_bucketed'] = (tx['amount'] / 10000).round().astype('long')
    return df[['amount_bucketed']]

# Bucket the transaction type as credit, debit, or transfer
# This is a reusable transformation you can use in multiple feature views
@transformation(mode="pandas")
def tx_type_bucketed_transformation(pdf: pandas.DataFrame):
    import pandas as pd
    df = pd.DataFrame()
    pdf['type_bucketed'] = 'unknown'
    pdf.loc[pdf.type_CASH_IN > 0, 'type_bucketed'] = 'credit'
    pdf.loc[pdf.type_CASH_OUT + pdf.type_DEBIT + pdf.type_PAYMENT > 0, 'type_bucketed'] = 'debit'
    pdf.loc[pdf.type_TRANSFER > 0, 'type_bucketed'] = 'transfer'
    df['type_bucketed'] = pdf['type_bucketed']
    return df

# We combine the results of our two other transformers (type and amount bucketed) here in
# a third transformer
@transformation(mode='pandas')
def merging_dfs(amount_bucketed_df: pandas.DataFrame, type_bucketed_df: pandas.DataFrame):
    import pandas as pd
    df = pd.DataFrame()
    df['amount_bucketed'] = amount_bucketed_df['amount_bucketed']
    df['type_bucketed'] = type_bucketed_df['type_bucketed']
    return df


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
    return merging_dfs(
        tx_amount_bucketed_transformation(transaction_request),
        tx_type_bucketed_transformation(transaction_request))

