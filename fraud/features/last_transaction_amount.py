
from tecton import FeatureTable
from core.entities import user
from pyspark.sql.types import StringType, StructType, StructField, LongType, TimestampType

schema = StructType()
schema.add(StructField("user_id", StringType()))
schema.add(StructField("last_transaction_amount", LongType()))
schema.add(StructField("timestamp_key", TimestampType()))

user_feature_table = FeatureTable(
    name="last_transaction_amount",
    description="Maintains the last transaction amount for a user.",
    entities=[user],
    schema=schema,
    online=True,
    offline=True,
    ttl='30 day'
)
