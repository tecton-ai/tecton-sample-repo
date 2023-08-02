from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.customers import customers
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(customers)],
    entities=[user],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2016, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    timestamp_field="signup_timestamp",
    description="User credit card issuer derived from the user credit card number.",
)
def user_credit_card_issuer(customers):
    return f"""
        SELECT
            user_id,
            signup_timestamp,
            CASE SUBSTRING(CAST(cc_num AS STRING), 0, 1)
                WHEN '4' THEN 'Visa'
                WHEN '5' THEN 'MasterCard'
                WHEN '6' THEN 'Discover'
                ELSE 'other'
            END as user_credit_card_issuer
        FROM
            {customers}
        """
