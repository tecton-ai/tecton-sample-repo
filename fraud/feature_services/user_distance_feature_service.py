from tecton import FeatureService
from fraud.features.on_demand_feature_views.user_to_user_distance import user_to_user_distance
from fraud.features.on_demand_feature_views.current_transaction_features import transaction_amount_features
from fraud.features.on_demand_feature_views.current_transaction_features import transaction_user_features

user_distance_feature_service = FeatureService(
    name='user_distance_feature_service',
    description='Feature Service for computing distance related features based on user location.',
    features=[user_to_user_distance, transaction_amount_features, transaction_user_features],
    on_demand_environment='tecton-python-extended:0.2',
    online_serving_enabled=True
)