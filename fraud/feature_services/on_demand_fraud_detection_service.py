from tecton import FeatureService
from fraud.features.stream_window_aggregate_feature_views.user_transaction_amount_metrics import user_transaction_amount_metrics
from fraud.features.user_transaction_counts import user_transaction_counts
from fraud.features.transaction_amount_is_high import transaction_amount_is_high
from fraud.features.transaction_amount_is_higher_than_average import transaction_amount_is_higher_than_average
from fraud.features.transaction_bucketing import transaction_bucketing
from fraud.features.stream_feature_views.last_transaction_amount_sql import last_transaction_amount_sql

# This feature service combines Batch, Streaming, and OnDemand Features.
# Accessing features from this service requires a RequestDataSource.
on_demand_fraud_detection_feature_service = FeatureService(
    name='on_demand_fraud_detection_feature_service',
    features=[
        transaction_amount_is_high,
        transaction_amount_is_higher_than_average,
        user_transaction_amount_metrics,
        transaction_bucketing,
        user_transaction_counts,
        last_transaction_amount_sql
    ]
)
