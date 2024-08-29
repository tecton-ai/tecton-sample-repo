from tecton import Aggregate, AggregationLeadingEdge, stream_feature_view
from tecton.types import Float64, Field
from tecton.v09_compat import FilteredSource, Aggregation
from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


# The following defines several sliding time window aggregations over a user's transaction amounts
@stream_feature_view(
    source=FilteredSource(transactions_stream),
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=10),  # Defines how frequently feature values get updated in the online store
    batch_schedule=timedelta(days=1), # Defines how frequently batch jobs are scheduled to ingest into the offline store
    features=[Aggregate(input_column=Field("amt", Float64), function="sum", time_window=timedelta(hours=1)), Aggregate(input_column=Field("amt", Float64), function="sum", time_window=timedelta(days=1)), Aggregate(input_column=Field("amt", Float64), function="sum", time_window=timedelta(days=3)), Aggregate(input_column=Field("amt", Float64), function="mean", time_window=timedelta(hours=1)), Aggregate(input_column=Field("amt", Float64), function="mean", time_window=timedelta(days=1)), Aggregate(input_column=Field("amt", Float64), function="mean", time_window=timedelta(days=3))],
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Transaction amount statistics and total over a series of time windows, updated every 10 minutes.',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME,
    timestamp_field="timestamp"
)
def user_transaction_amount_metrics(transactions):
    return f'''
        SELECT
            user_id,
            amt,
            timestamp
        FROM
            {transactions}
        '''
