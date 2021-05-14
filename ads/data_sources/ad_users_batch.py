from tecton import HiveDSConfig, BatchDataSource

ad_users_batch = BatchDataSource(
    name="ad_users_batch",
    batch_ds_config=HiveDSConfig(
        database='ad_impressions_2',
        table='users_part_00000_tid_1603063917041543168_ef2df2b1_dcdc_4526_9666_f76bb53cb144_12614_1_c000_snappy_parquet',
        # timestamp_column_name='signup_date',
    ),
    family='ad_serving',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
