from tecton import FeatureService
from features.batch_feature_views.user_transaction_counts import user_transaction_counts
from features.stream_feature_views.user_last_transaction_amount import user_last_transaction_amount
from features.on_demand_feature_views.transaction_amount_is_high import transaction_amount_is_high
from features.on_demand_feature_views.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
from features.batch_feature_views.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d
from features.batch_feature_views.user_has_good_credit import user_has_good_credit_pyspark

fraud_detection_feature_service = FeatureService(
    name='fraud_detection_feature_service',
    features=[
        user_last_transaction_amount,
        transaction_amount_is_high,
        transaction_amount_is_higher_than_average,
        user_transaction_counts,
        user_distinct_merchant_transaction_count_30d,
        user_has_good_credit_pyspark
    ]
)
