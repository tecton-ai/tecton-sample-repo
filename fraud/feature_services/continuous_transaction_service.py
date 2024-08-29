from tecton.v09_compat import FeatureService
from fraud.features.stream_features.user_continuous_transaction_count import user_continuous_transaction_count

continuous_feature_service = FeatureService(
    name='continuous_feature_service',
    description='A FeatureService providing continuous features.',
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    online_serving_enabled=False,
    features=[
        user_continuous_transaction_count
    ]
)
