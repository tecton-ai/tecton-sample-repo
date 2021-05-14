from tecton import FeatureService
from fraud.features.user_transaction_amount_metrics import user_transaction_amount_metrics
from fraud.features.user_transaction_counts import user_transaction_counts
from fraud.features.transaction_amount_is_high import transaction_amount_is_high
from core.features.user_date_of_birth import user_date_of_birth
from core.features.user_age import user_age
from fraud.features.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
# from fraud.features.user_has_good_credit import user_has_good_credit

fraud_detection_feature_service = FeatureService(
    name='fraud_detection_feature_service',
    description='A FeatureService providing features for a model that predicts if a transaction is fraudulent.',
    family='fraud',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    features=[
        transaction_amount_is_high,
        transaction_amount_is_higher_than_average,
        user_transaction_amount_metrics,
        user_transaction_counts
    ]
)

# fraud_detection_feature_service_v2 = FeatureService(
#     name='fraud_detection_feature_service:v2',
#     description='A FeatureService providing features for a model that predicts if a transaction is fraudulent.',
#     family='fraud',
#     tags={'release': 'production'},
#     owner="matt@tecton.ai",
#     features=[
#         user_has_good_credit, # New feature
#         transaction_amount_is_high,
#         transaction_amount_is_higher_than_average,
#         user_transaction_amount_metrics,
#         user_transaction_counts,
#
#     ]
# )
