from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, StringType, TimestampType
from tecton import Entity, FeatureTable, DeltaConfig
from fraud.entities import user


schema = StructType()
schema.add(StructField('user_id', StringType()))
schema.add(StructField('timestamp', TimestampType()))
schema.add(StructField('user_embedding', ArrayType(FloatType())))


user_embeddings = FeatureTable(
    name='user_embeddings',
    entities=[user],
    schema=schema,
    online=True,
    offline=True,
    ttl='10day',
    description='Precomputed user embeddings pushed into Tecton.'
)