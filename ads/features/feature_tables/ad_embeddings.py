from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, StringType, TimestampType
from tecton import Entity, FeatureTable, DeltaConfig
from ads.entities import ad


schema = StructType([
    StructField('ad_id', StringType()),
    StructField('timestamp', TimestampType()),
    StructField('ad_embedding', ArrayType(FloatType()))
])


ad_embeddings = FeatureTable(
    name='ad_embeddings',
    entities=[ad],
    schema=schema,
    online=True,
    offline=True,
    ttl='10day',
    description='Precomputed ad embeddings pushed into Tecton.',
    owner='jake@tecton.ai'
)
