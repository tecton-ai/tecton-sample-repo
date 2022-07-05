from tecton import FeatureService
from fraud.features.stream_features.user_transaction_amount_metrics import user_transaction_amount_metrics
from fraud.features.batch_features.user_transaction_counts import user_transaction_counts
from fraud.features.on_demand_feature_views.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
from fraud.features.batch_features.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d
from fraud.features.batch_features.merchant_fraud_rate import merchant_fraud_rate


fraud_detection_feature_service = FeatureService(
    name='fraud_detection_feature_service',
    features=[
        transaction_amount_is_higher_than_average,
        user_transaction_amount_metrics,
        user_transaction_counts,
        user_distinct_merchant_transaction_count_30d,
        merchant_fraud_rate
    ]
)

# # fraud_detection_feature_service_v2 = FeatureService(
# #     name='fraud_detection_feature_service:v2',
# #     description='A FeatureService providing features for a model that predicts if a transaction is fraudulent.',
# #     tags={'release': 'production'},
# #     features=[
# #         user_has_great_credit, # New feature
# #         last_transaction_amount_sql,
# #         transaction_amount_is_high,
# #         transaction_amount_is_higher_than_average,
# #         user_transaction_amount_metrics,
# #         user_transaction_counts,
# #         user_distinct_merchant_transaction_count_30d
# #     ]
# # )
#
# minimal_fs = FeatureService(
#     name='minimal_fs',
#     features=[
#         transaction_amount_is_high
#     ]
# )
