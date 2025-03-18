from tecton import FeatureService
from fraud.features.stream_features.user_transaction_amount_metrics import user_transaction_amount_metrics
from fraud.features.batch_features.user_transaction_counts import user_transaction_counts
from fraud.features.realtime_features.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
from fraud.features.batch_features.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d
from fraud.features.batch_features.merchant_fraud_rate import merchant_fraud_rate


fraud_detection_feature_service = FeatureService(
    name='fraud_detection_feature_service',
    prevent_destroy=False,  # Set to True for production services to prevent accidental destructive changes or downtime.
    features=[
        transaction_amount_is_higher_than_average,
        user_transaction_amount_metrics,
        user_transaction_counts,
        user_distinct_merchant_transaction_count_30d,
        merchant_fraud_rate
    ]
)
