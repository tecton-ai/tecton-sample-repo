import math

from ads.features.on_demand_feature_views.user_query_embedding_similarity import user_query_embedding_similarity


# Testing the 'user_query_embedding_similarity' feature which takes in request data ('query_embedding')
# and a precomputed feature ('user_embedding') as inputs
def test_user_query_embedding_similarity():
    request = {'query_embedding': [1.0, 1.0, 0.0]}
    user_embedding = {'user_embedding': [0.0, 1.0, 1.0]}

    actual = user_query_embedding_similarity.test_run(request=request, user_embedding=user_embedding)

    # Float comparison.
    expected = 0.5
    assert math.isclose(actual['cosine_similarity'], expected)
