from tecton import BatchSource, FileConfig

transactions = BatchSource(
    name="transactions",
    batch_config=FileConfig(
        uri="s3://tecton.ai.public/tutorials/fraud_demo/transactions/data.pq",
        file_format="parquet",
        timestamp_field="timestamp",
    ),
)