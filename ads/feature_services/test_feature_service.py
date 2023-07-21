from tecton import FeatureService
from ads.features.on_demand_feature_views.similarity import fuzzy_similarity
from ads.features.on_demand_feature_views.permutations import permutations_combinations
from ads.features.on_demand_feature_views.click_count_is_high import click_count_is_high
from ads.features.batch_features.ad_auction_keywords import auction_keywords


test_feature_service = FeatureService(
    name='test_feature_service',
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    on_demand_environment="tecton-python-extended:0.1",
    online_serving_enabled=False,
    features=[
       fuzzy_similarity,
       permutations_combinations,
       click_count_is_high,
    ],
)
