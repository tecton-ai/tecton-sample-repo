import numpy as np
from ads.features.on_demand_feature_views.user_ad_embedding_similarity import user_ad_embedding_similarity


def test_user_ad_embedding_similarity():
    # Test case 1: Valid embeddings with known similarity
    ad_embedding = {"ad_embedding": np.array([1.0, 0.0, 0.0])}
    user_embedding = {"user_embedding": np.array([0.5, 0.5, 0.0])}
    
    # Expected cosine similarity = (1*0.5 + 0*0.5 + 0*0) / (sqrt(1) * sqrt(0.5)) = 0.5 / sqrt(0.5) ≈ 0.7071
    expected_similarity = 0.7071067811865475
    
    input_data = {
        "ad_embedding": ad_embedding,
        "user_embedding": user_embedding
    }
    
    result = user_ad_embedding_similarity.run_transformation(input_data)
    assert np.isclose(result["cosine_similarity"], expected_similarity)
    
    # Test case 2: Missing embeddings
    input_data = {
        "ad_embedding": {"ad_embedding": None},
        "user_embedding": user_embedding
    }
    result = user_ad_embedding_similarity.run_transformation(input_data)
    assert result["cosine_similarity"] == -1.0
    
    input_data = {
        "ad_embedding": ad_embedding,
        "user_embedding": {"user_embedding": None}
    }
    result = user_ad_embedding_similarity.run_transformation(input_data)
    assert result["cosine_similarity"] == -1.0
    
    input_data = {
        "ad_embedding": {"ad_embedding": None},
        "user_embedding": {"user_embedding": None}
    }
    result = user_ad_embedding_similarity.run_transformation(input_data)
    assert result["cosine_similarity"] == -1.0
    
    # Test case 3: Orthogonal vectors (should have similarity 0)
    ad_embedding = {"ad_embedding": np.array([1.0, 0.0])}
    user_embedding = {"user_embedding": np.array([0.0, 1.0])}
    
    input_data = {
        "ad_embedding": ad_embedding,
        "user_embedding": user_embedding
    }
    result = user_ad_embedding_similarity.run_transformation(input_data)
    assert np.isclose(result["cosine_similarity"], 0.0) 