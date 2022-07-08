from tecton.compat import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, DoubleType
from ads.features.feature_tables.user_embeddings import user_embeddings
from ads.features.feature_tables.ad_embeddings import ad_embeddings
import pandas

output_schema = StructType([
    StructField('cosine_similarity', DoubleType())
])

@on_demand_feature_view(
    inputs={
        'ad_embedding': Input(ad_embeddings),
        'user_embedding': Input(user_embeddings)
    },
    mode='python',
    output_schema=output_schema,
    family='fraud',
    owner='jake@tecton.ai',
    tags={'release': 'production'},
    description="Computes the cosine similarity between a precomputed ad embedding and a precomputed user embedding."
)
def user_ad_embedding_similarity(ad_embedding, user_embedding):
    import numpy as np
    from numpy.linalg import norm

    @np.vectorize
    def cosine_similarity(a: np.ndarray, b: np.ndarray):
        # Handle the case where one or both entities do not have a precomputed embedding.
        if a is None or b is None:
            return -1.0

        return np.dot(a, b)/(norm(a)*norm(b))

    result = {}
    result["cosine_similarity"] = cosine_similarity(user_embedding["user_embedding"], ad_embedding["ad_embedding"]).astype('float64')
    return result
