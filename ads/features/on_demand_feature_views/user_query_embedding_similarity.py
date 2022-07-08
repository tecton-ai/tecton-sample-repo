from tecton.compat import RequestDataSource, Input, on_demand_feature_view
from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, DoubleType
from ads.features.feature_tables.user_embeddings import user_embeddings
import pandas


request_schema = StructType([
    StructField('query_embedding', ArrayType(FloatType()))
])
request = RequestDataSource(request_schema=request_schema)

output_schema = StructType([
    StructField('cosine_similarity', DoubleType())
])


@on_demand_feature_view(
    inputs={
        'request': Input(request),
        'user_embedding': Input(user_embeddings)
    },
    mode='python',
    output_schema=output_schema,
    family='fraud',
    owner='jake@tecton.ai',
    tags={'release': 'production'},
    description="Computes the cosine similarity between a query embedding and a precomputed user embedding."
)
def user_query_embedding_similarity(request, user_embedding):
    import numpy as np
    from numpy.linalg import norm

    @np.vectorize
    def cosine_similarity(a: np.ndarray, b: np.ndarray):
        # Handle the case where there is no precomputed user embedding.
        if a is None or b is None:
            return -1.0

        return np.dot(a, b)/(norm(a)*norm(b))

    result = {}
    result["cosine_similarity"] = cosine_similarity(user_embedding["user_embedding"], request["query_embedding"]).astype('float64')
    return result
