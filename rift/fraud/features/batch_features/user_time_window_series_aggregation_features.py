from tecton import batch_feature_view, Aggregate, TimeWindowSeries
from datetime import timedelta
from fraud.data_sources.transactions import transactions_batch
from fraud.entities import user
from tecton.types import Field, Int32

@batch_feature_view(
    sources=[transactions_batch],
    mode="pandas",
    entities=[user],
    aggregation_interval=timedelta(days=1),
    description="Daily User transaction sums over the past 7 days",
    timestamp_field="timestamp",
    features=[
        Aggregate(
            input_column=Field("value", Int32),
            function="sum",
            time_window=TimeWindowSeries(
                series_start=timedelta(days=-7),
                window_size=timedelta(days=1),
            ),
            description="An array of sums of transactions for every day over the past 7 days"
        )
    ],
)
def user_transaction_sums(transactions):
    return transactions[['user_id', 'timestamp', 'value']]

