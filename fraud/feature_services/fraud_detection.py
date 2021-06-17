from tecton import FeatureService
from fraud.features.stream_window_aggregate_feature_views.user_transaction_amount_metrics import user_transaction_amount_metrics
from fraud.features.user_transaction_counts import user_transaction_counts
from fraud.features.transaction_amount_is_high import transaction_amount_is_high
from fraud.features.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
from fraud.features.stream_window_aggregate_feature_views.continuous_fraudulent_transactions_count import *
from fraud.features.stream_window_aggregate_feature_views.continuous_non_fraudulent_transactions_count import *
from fraud.features.transaction_bucketing import transaction_bucketing
from fraud.features.stream_feature_views.last_transaction_amount_sql import last_transaction_amount_sql
# from fraud.features.user_has_good_credit import user_has_good_credit


fraud_detection_feature_service = FeatureService(
    name='fraud_detection_feature_service',
    features=[
        transaction_amount_is_high,
        transaction_amount_is_higher_than_average,
        user_transaction_amount_metrics,
        transaction_bucketing,
        user_transaction_counts,
        last_transaction_amount_sql
    ]
)


# fraud_detection_feature_service_v2 = FeatureService(
#     name='fraud_detection_feature_service:v2',
#     description='A FeatureService providing features for a model that predicts if a transaction is fraudulent.',
#     family='fraud',
#     tags={'release': 'production'},
#     features=[
#         user_has_good_credit, # New feature
#         transaction_amount_is_high,
#         transaction_amount_is_higher_than_average,
#         user_transaction_amount_metrics,
#         transaction_bucketing,
#         user_transaction_counts,
#     ]
# )


continuous_feature_service = FeatureService(
    name='continuous_feature_service',
    description='A FeatureService providing continuous features.',
    family='fraud',
    tags={'release': 'production'},
    owner='kevin@tecton.ai',
    features=[
        continuous_non_fraudulent_transactions_count,
        continuous_fraudulent_transactions_count,
        last_transaction_amount_sql
    ]
)
