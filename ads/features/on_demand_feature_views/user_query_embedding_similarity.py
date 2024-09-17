from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import Field, Array, Float64
from ads.features.feature_tables.user_embeddings import user_embeddings


request_schema = [Field('query_embedding', Array(Float64))]
request = RequestSource(schema=request_schema)

@realtime_feature_view(
    sources=[request, user_embeddings],
    mode='python',
    features=[Attribute(name='cosine_similarity', dtype=Float64)],
    owner='demo-user@tecton.ai',
    tags={'release': 'production'},
    description="Computes the cosine similarity between a query embedding and a precomputed user embedding."
)
def user_query_embedding_similarity(request, user_embedding):
    import numpy as np
    from numpy.linalg import norm

    def cosine_similarity(a: np.ndarray, b: np.ndarray):
        # Handle the case where there is no precomputed user embedding.
        if a is None or b is None:
            return -1.0

        return np.dot(a, b)/(norm(a)*norm(b))

    result = {}
    result["cosine_similarity"] = cosine_similarity(user_embedding["user_embedding"], request["query_embedding"]).astype('float64')
    return result
