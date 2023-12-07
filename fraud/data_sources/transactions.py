from tecton.types import Field, Int64, String, Timestamp, Float64
from tecton import PushConfig, StreamSource, FileConfig

schema = [
            Field(name="user_id", dtype=String),
            Field(name="timestamp", dtype=Timestamp),
            Field(name="amt", dtype=Float64),
]

batch_config = FileConfig(
        uri='s3://tecton.ai.public/tutorials/fraud_demo/transactions/data.pq',
        timestamp_field='timestamp',
        file_format='parquet'
    )

stream_config = PushConfig(log_offline=False)
stream_config_log = PushConfig(log_offline=True)

ingest_source = StreamSource(
    name="ingest_ds",
    schema=schema,
    stream_config=stream_config,
    batch_config=batch_config,
    description="Sample Stream Source for click events",
    owner="pooja@tecton.ai",
)

ingest_source_no_batch = StreamSource(
    name="ingest_ds_no_batch",
    schema=schema,
    stream_config=stream_config_log,
    description="Sample Stream Source for click events",
    owner="pooja@tecton.ai",
)