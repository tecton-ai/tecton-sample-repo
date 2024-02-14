from tecton.types import Field, Int64, String, Timestamp, Float64, Array, Struct, Bool, Map
from tecton import PushConfig, StreamSource, FileConfig, HiveConfig, DatetimePartitionColumn

input_schema = [
    Field(name="content_keyword", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field(name="clicked", dtype=Int64),
    Field(name="clicked_str", dtype=String),
]

simple_struct = Struct(
    [
        Field("clicked", Int64),
        Field("clicked_str", String)
    ]
)

struct_of_structs = Struct(
    [
        Field("struct_1", simple_struct),
        Field("struct_2", simple_struct),
        Field("struct_3", simple_struct),
    ]
)

struct_of_arrays_type = Struct(
            [
                Field("float64_array", Array(Float64)),
            ]
        )

input_schema_with_struct = [
    Field(name="content_keyword", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field("simple_struct", simple_struct)
    #Field("array_of_structs", Array(simple_struct)),
    #Field("struct_of_structs", struct_of_structs),
    #Field("struct_of_arrays", struct_of_arrays_type),
]


def type_caster(df):
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType
    from pyspark.sql.types import StringType

    df = (
        df.withColumn("content_keyword", col("content_keyword").cast(StringType()))
        .withColumn("clicked", col("clicked").cast(LongType()))
        .withColumn("clicked_str", col("clicked").cast(StringType()))
    )
    return df


batch_config = HiveConfig(
    database='demo_ads',
    table='impressions_batch',
    timestamp_field='timestamp',
    #datetime_partition_columns=[
      #  DatetimePartitionColumn(column_name="datestr", datepart="date", zero_padded=True)
    #],
    post_processor=type_caster,
)

stream_config = PushConfig()

ad_ingest_batch = StreamSource(
    name="ad_ingest_batch",
    schema=input_schema,
    batch_config=batch_config,
    stream_config=stream_config,
    description="Sample Push Source for click events",
)


def post_processor_ingest(df):
    df['clicked_str'] = df['clicked'].astype(str)
    return df


post_processor_schema = [
    Field(name="content_keyword", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field(name="clicked", dtype=Int64),
]

stream_config = PushConfig(
    log_offline=True,
    post_processor=post_processor_ingest,
    post_processor_mode="pandas",
    input_schema=post_processor_schema,
)

ad_ingest_no_batch = StreamSource(
    name="ad_ingest_no_batch",
    schema=input_schema,
    stream_config=stream_config,
    description="Sample Stream Source for click events",
)

ingest_struct = StreamSource(
    name="ingest_ds_struct",
    schema=input_schema_with_struct,
    stream_config=PushConfig(log_offline=True),
    description="Sample Push Source with a struct schema",
)

fo_schema = [
    Field(name="fulfillment_option_id", dtype=Int64),
    Field(name="timestamp_key", dtype=Timestamp),
    Field(name="quote_id", dtype=Int64),
    Field(name="selected", dtype=Bool),
    Field(name="deleted", dtype=Bool)
]
fulfillment_option_source = StreamSource(
    name="fulfillment_option_source",
    schema=fo_schema,
    stream_config=PushConfig(log_offline=True),
    description="Sample Push Source with a struct schema",
)

# Example Struct for product details
product_details_struct = Struct(
    [
        Field("product_id", dtype=String),
        Field("category", dtype=String),
        Field("price", dtype=Float64),
    ]
)

# Schema with Struct and Map for Stream Source
schema = [
    Field('user_id', dtype=String),
    Field('timestamp', dtype=Timestamp),
    Field('product_details', dtype=product_details_struct),
    Field("user_preferences", Map(String, Float64)), # Map product category to a user preference score
]

# Stream Source
ecommerce_source = StreamSource(
    name='ecommerce_events',
    schema=schema,
    stream_config=PushConfig(log_offline=True),
    description='Stream of e-commerce events, including user actions and product details.'
)
