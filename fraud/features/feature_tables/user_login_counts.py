from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType
from tecton.compat import Entity, FeatureTable, DeltaConfig
from fraud.entities import user


schema = StructType([
    StructField('user_id', StringType()),
    StructField('timestamp', TimestampType()),
    StructField('user_login_count_7d', LongType()),
    StructField('user_login_count_30d', LongType()),
])


user_login_counts = FeatureTable(
    name='user_login_counts',
    entities=[user],
    schema=schema,
    online=False,
    offline=True,
    ttl='30day',
    owner='derek@tecton.ai'
)
