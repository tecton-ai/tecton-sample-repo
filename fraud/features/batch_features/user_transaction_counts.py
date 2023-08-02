from tecton import batch_feature_view, FilteredSource, Aggregation
from entities import user
from data_sources.transactions import transactions
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(transactions)],
    entities=[user],
    mode="spark_sql",
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column="transaction_id", function="count", time_window=timedelta(days=1)),
        Aggregation(column="transaction_id", function="count", time_window=timedelta(days=30)),
        Aggregation(column="transaction_id", function="count", time_window=timedelta(days=90)),
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    description="User transaction totals over a series of time windows, updated daily.",
    name="user_transaction_counts",
)
def user_transaction_counts(transactions):
    return f"""
        SELECT
            user_id,
            transaction_id,
            timestamp
        FROM
            {transactions}
        """
