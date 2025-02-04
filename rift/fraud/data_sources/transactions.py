from tecton import FileConfig, KinesisConfig, StreamSource, BatchSource, DatetimePartitionColumn
from datetime import timedelta

from tecton.types import Field, Float64, String, Timestamp, Struct, Map, Int64
from tecton import StreamSource, PushConfig


batch_config = FileConfig(
    uri='s3://tecton.ai.public/tutorials/fraud_demo/transactions/',
    file_format='parquet',
    timestamp_field='timestamp'
)

schema = [
    Field("user_id", dtype=String),
    Field("transaction_id", dtype=String),
    Field("category", dtype=String),
    Field("amt", dtype=Float64),        # cast to 'double'
    Field("is_fraud", dtype=Int64),     # cast to 'long'
    Field("merchant", dtype=String),
    Field("merch_lat", dtype=Float64),  # cast to 'double'
    Field("merch_long", dtype=Float64), # cast to 'double'
    Field("timestamp", dtype=Timestamp)  # from_utc_timestamp converts to timestamp
]


# Stream Source
transactions_stream = StreamSource(
    name="transactions_stream",
    schema=schema,
    stream_config=PushConfig(),
    description="Stream of transaction events.",
    batch_config=batch_config
)

transactions_batch = BatchSource(
    name='transactions_batch',
    batch_config=batch_config,
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)
