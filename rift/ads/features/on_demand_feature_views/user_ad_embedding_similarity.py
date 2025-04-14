from tecton import realtime_feature_view, Attribute
from tecton.types import Float64
from ads.features.feature_tables.user_embeddings import user_embeddings
from ads.features.feature_tables.ad_embeddings import ad_embeddings


@realtime_feature_view(
    sources=[ad_embeddings, user_embeddings],
    mode='python',
    features=[
        Attribute(name='cosine_similarity', dtype=Float64)
    ],
    owner='demo-user@tecton.ai',
    tags={'release': 'production'},
    description="Computes the cosine similarity between a precomputed ad embedding and a precomputed user embedding."
)
def user_ad_embedding_similarity(ad_embedding, user_embedding):
    import numpy as np
    from numpy.linalg import norm

    def cosine_similarity(a: np.ndarray, b: np.ndarray):
        # Handle the case where one or both entities do not have a precomputed embedding.
        if a is None or b is None:
            return -1.0

        return np.dot(a, b)/(norm(a)*norm(b))

    result = {}
    # Handle the case where the input dictionaries are None or missing the embedding key
    user_emb = user_embedding.get("user_embedding") if user_embedding else None
    ad_emb = ad_embedding.get("ad_embedding") if ad_embedding else None
    similarity = cosine_similarity(user_emb, ad_emb)
    result["cosine_similarity"] = float(similarity)  # Convert to float to ensure it's JSON serializable
    return result
