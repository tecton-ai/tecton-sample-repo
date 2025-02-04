from tecton import BatchSource, FileConfig

sessions_batch = BatchSource(
    name='sessions_batch',
    batch_config=FileConfig(
        uri='s3://tecton.ai.public/tutorials/recsys-demo/train.parquet',
        file_format='parquet',
        timestamp_field='ts'
    ),
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)


