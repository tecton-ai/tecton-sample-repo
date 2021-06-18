from tecton import HiveDSConfig, BatchDataSource


credit_scores_batch = BatchDataSource(
    name='credit_scores_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='fraud_credit_scores',
        timestamp_column_name='timestamp'
    ),
    family='fraud_detection',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
