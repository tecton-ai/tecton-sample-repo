from tecton import spark_batch_config, BatchSource

@spark_batch_config()
def sessions_data_source_function(spark):
    from pyspark.sql.types import StructField, StructType, IntegerType, TimestampType, StringType
    schema = StructType([
        StructField("session", IntegerType(), True),
        StructField("aid", IntegerType(), True),
        StructField("ts", TimestampType(), True),
        StructField("type", StringType(), True)
    ])

    df = spark.read.schema(schema).parquet("s3://tecton.ai.public/tutorials/recsys-demo/train.parquet")
    return df

sessions_batch = BatchSource(
    name="sessions_batch",
    batch_config=sessions_data_source_function,
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

