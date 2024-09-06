from tecton.types import Field, Timestamp, Array, Float64, Int64
from tecton import FeatureTable, Attribute
from datetime import timedelta

from ads.entities import ad

schema = [
    Field('ad_id', Int64),
    Field('timestamp', Timestamp),
    Field('ad_embedding', Array(Float64))
]


ad_embeddings = FeatureTable(
    name='ad_embeddings',
    entities=[ad],
    online=True,
    offline=True,
    ttl=timedelta(days=10),
    description='Precomputed ad embeddings pushed into Tecton.',
    owner='demo-user@tecton.ai',
    timestamp_field="timestamp",
    features=[
        Attribute("ad_embedding", dtype=Array(Float64))
    ],
)
