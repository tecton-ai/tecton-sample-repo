from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from tecton import FeatureService, FeatureView, OnDemandFeatureView
from tecton.types import Field, Float64, Int64, String, Timestamp
from tecton import transformation
from tecton import materialization_context

from features.user_transaction_aggregation import user_transaction_aggregation
from features.user_transaction_aggregation import user_transaction_aggregation_odfv

# Define the feature service
user_transaction_aggregation_service = FeatureService(
    name="user_transaction_aggregation_service",
    features=[user_transaction_aggregation, user_transaction_aggregation_odfv],
    online_serving_enabled=True,
    offline_serving_enabled=True,
    description="A feature service for user transaction aggregation features",
) 