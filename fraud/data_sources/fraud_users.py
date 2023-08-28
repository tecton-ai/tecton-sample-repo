from tecton import FileConfig, BatchSource


fraud_users_batch = BatchSource(
    name='users_batch',
    batch_config=FileConfig(
        uri='s3://tecton.ai.public/tutorials/fraud_demo/customers/',
        file_format='parquet',
        timestamp_field='signup_timestamp'
    ),
    owner='david@tecton.ai',
    tags={'release': 'production'}
)
