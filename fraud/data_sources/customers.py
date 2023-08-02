from tecton import BatchSource, FileConfig

customers = BatchSource(
    name="customers",
    batch_config=FileConfig(
        uri="s3://tecton.ai.public/tutorials/fraud_demo/customers/data.pq",
        file_format="parquet",
        timestamp_field="signup_timestamp",
    ),
)
