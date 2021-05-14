from tecton.feature_views import stream_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input
from tecton import FeatureAggregation
from global_entities import user
from fraud.data_sources.transactions_stream import transactions_stream
from datetime import datetime

@stream_window_aggregate_feature_view(
    inputs={"transactions": Input(transactions_stream)},
    entities=[user],
    mode="spark_sql",
    aggregation_slide_period="1h",
    aggregations=[
        FeatureAggregation(column="amount", function="mean", time_windows=["1h", "12h", "24h","72h","168h", "960h"]),
        FeatureAggregation(column="amount", function="sum", time_windows=["1h", "12h", "24h","72h","168h", "960h"])
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    family='fraud',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    description="Average transaction amount and total over a series of time windows, updated daily."
)
def user_transaction_amount_metrics(transactions):
    return f"""
        SELECT
            nameorig as user_id,
            amount,
            timestamp
        FROM
            {transactions}
        """
