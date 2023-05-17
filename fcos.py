from tecton import spark_batch_config
from tecton import BatchSource

@spark_batch_config(supports_time_filtering=True)
def transaction_created_processor(spark, filter_context):
    from pyspark.sql.functions import col

    df = spark.sql(
        """
        SELECT user_uuid, created_at, transaction_amount
        FROM my_database.transactions
    """
    )

    ts_column = "created_at"
    df = df.withColumn(ts_column, col(ts_column).cast("timestamp"))

    # Handle time filtering
    if filter_context:
        if filter_context.start_time:
            df = df.where(col(ts_column) >= filter_context.start_time)
        if filter_context.end_time:
            df = df.where(col(ts_column) < filter_context.end_time)
    return df

mock_batch_source = BatchSource(name="ds", batch_config=transaction_created_processor)