from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType
from tecton import Entity, FeatureTable, DeltaConfig
from core.entities import user


schema = StructType()
schema.add(StructField("user_id", StringType()))
schema.add(StructField("timestamp", TimestampType()))
schema.add(StructField("user_login_count_7d", LongType()))
schema.add(StructField("user_login_count_30d", LongType()))


user_login_counts = FeatureTable(
    name="user_login_counts",
    entities=[user],
    schema=schema,
    online=True,
	offline=True,
	ttl='30day'
)
