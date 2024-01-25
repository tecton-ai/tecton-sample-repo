from tecton import spark_batch_config, BatchSource

# Public BigQuery Dataset
INTERNATIONAL_TOP_TERMS_TABLE = "bigquery-public-data.google_trends.international_top_terms"


@spark_batch_config(supports_time_filtering=True)
def data_source_function(spark, filter_context):
    from pyspark.sql.functions import col
    df = (
            spark.read.format("com.google.cloud.spark.bigquery")
            .option("table", INTERNATIONAL_TOP_TERMS_TABLE)
            .load()
    )

    # apply partition filtering for materialization
    partition_column = "refresh_date"
    if filter_context:
        if filter_context.start_time:
            df = df.where(col(partition_column) >= filter_context.start_time)
        if filter_context.end_time:
            df = df.where(col(partition_column) < filter_context.end_time)
    return df


top_terms = BatchSource(
    name="top_terms",
    batch_config=data_source_function,
    description="Public dataset of rising queries from Google Trends."
)
