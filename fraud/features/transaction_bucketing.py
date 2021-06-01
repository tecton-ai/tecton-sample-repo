# Pipelines and Modularized Transformations (On-Demand)
import pandas
from tecton import transformation, on_demand_feature_view, Input, RequestDataSource
from pyspark.sql.types import StructType, StructField, StringType, LongType

request_schema = StructType([
    StructField('amount', LongType()),
    StructField('type_CASH_IN', LongType()),
    StructField('type_CASH_OUT', LongType()),
    StructField('type_DEBIT', LongType()),
    StructField('type_PAYMENT', LongType()),
    StructField('type_TRANSFER', LongType())
])

output_schema = StructType([
    StructField('amount_bucketed', LongType()),
    StructField('type_bucketed', StringType())
])

# Bucket the transaction amount by $10,000 dollars
@transformation(mode="pandas")
def tx_amount_bucketed_transformation(tx: pandas.DataFrame):
    tx['amount_bucketed'] = (tx.amount / 10000).round().astype('long')
    return tx

# Bucket the transaction type as credit, debit, or transfer
@transformation(mode="pandas")
def tx_type_bucketed(pdf: pandas.DataFrame):
    pdf['type_bucketed'] = 'unknown'
    pdf.loc[pdf.type_CASH_IN > 0, 'type_bucketed'] = 'credit'
    pdf.loc[pdf.type_CASH_OUT + pdf.type_DEBIT + pdf.type_PAYMENT > 0, 'type_bucketed'] = 'debit'
    pdf.loc[pdf.type_TRANSFER > 0, 'type_bucketed'] = 'transfer'
    return pdf

# Create the online feature view that uses both the amount and the type bucket transformations
@on_demand_feature_view(
    description="[Online Feature] Bucket transaction amount and type",
    inputs={"request_data": Input(RequestDataSource(request_schema))},
    output_schema=output_schema,
    mode='pipeline'
)
def transaction_bucketing(request_data):
    request_data = tx_amount_bucketed_transformation(request_data)
    request_data = tx_type_bucketed(request_data)
    return request_data
