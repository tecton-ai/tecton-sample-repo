from tecton import FeatureService

from features.batch_features.user_credit_card_issuer import user_credit_card_issuer
from features.batch_features.user_transaction_counts import user_transaction_counts

from features.batch_features.user_home_location import user_home_location

from features.on_demand_features.transaction_amount_is_high import (
    transaction_amount_is_high,
)
from features.on_demand_features.transaction_distance_from_home import (
    transaction_distance_from_home,
)

fraud_detection_feature_service = FeatureService(
    name="fraud_detection_feature_service",
    online_serving_enabled=True,
    features=[
        user_credit_card_issuer,
        transaction_amount_is_high,
        user_transaction_counts,
        user_home_location,
        transaction_distance_from_home
    ],
)
