from tecton import FeatureService
from fraud.features.stream_window_aggregate_feature_views.continuous_fraudulent_transactions_count import *
from fraud.features.stream_window_aggregate_feature_views.continuous_non_fraudulent_transactions_count import *
from fraud.features.stream_feature_views.last_transaction_amount_sql import last_transaction_amount_sql

continuous_feature_service = FeatureService(
    name='continuous_feature_service',
    description='A FeatureService providing continuous features.',
    family='fraud',
    tags={'release': 'production'},
    owner='kevin@tecton.ai',
    online_serving_enabled=False,
    features=[
        continuous_non_fraudulent_transactions_count,
        continuous_fraudulent_transactions_count,
        last_transaction_amount_sql
    ]
)
