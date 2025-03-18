from tecton import stream_feature_view, Aggregate
from tecton.types import Field, Float64, String

from tecton.aggregation_functions import (
    approx_percentile,
    first,
    last,
    first_distinct,
    last_distinct,
)

from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta

# A comprehensive overview of all the aggregation functions available in Tecton
@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode="pandas",
    batch_schedule=timedelta(days=1),
    features=[
        Aggregate(
            input_column=Field("amt", Float64),
            function="sum",
            time_window=timedelta(hours=1),
            description="Sum of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="mean",
            time_window=timedelta(hours=1),
            description="Mean of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="count",
            time_window=timedelta(hours=1),
            description="Count of transactions over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="min",
            time_window=timedelta(hours=1),
            description="Minimum transaction amount over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="max",
            time_window=timedelta(hours=1),
            description="Maximum transaction amount over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="stddev_pop",
            time_window=timedelta(hours=1),
            description="Population standard deviation of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="stddev_samp",
            time_window=timedelta(hours=1),
            description="Sample standard deviation of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="var_pop",
            time_window=timedelta(hours=1),
            description="Population variance of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="var_samp",
            time_window=timedelta(hours=1),
            description="Sample variance of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function=approx_percentile(percentile=0.5, precision=100),
            time_window=timedelta(hours=1),
            description="Approximate 50th percentile (median) of transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function=first(2),
            time_window=timedelta(hours=1),
            description="First 2 transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function="last",
            time_window=timedelta(hours=1),
            description="Last transaction amount over the past hour.",
        ),
        Aggregate(
            input_column=Field("amt", Float64),
            function=last(2),
            time_window=timedelta(hours=1),
            description="Last 2 transaction amounts over the past hour.",
        ),
        Aggregate(
            input_column=Field("merchant", String),
            function=first_distinct(2),
            time_window=timedelta(hours=1),
            description="First 2 distinct merchants over the past hour.",
        ),
        Aggregate(
            input_column=Field("merchant", String),
            function=last_distinct(2),
            time_window=timedelta(hours=1),
            description="Last 2 distinct merchants over the past hour.",
        ),
    ],
    timestamp_field="timestamp",
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
)
def user_transaction_aggregation_features(transactions):
    return transactions[['user_id', 'amt', 'timestamp', "merchant"]]