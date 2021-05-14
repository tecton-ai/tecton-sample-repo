from tecton.feature_views import batch_feature_view, Input
from global_entities import user
from fraud.data_sources.fraud_users_batch import fraud_users_batch
from datetime import datetime

@batch_feature_view(
    inputs={"fraud_users": Input(fraud_users_batch)},
    entities=[user],
    mode="pyspark",
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 1, 1),
    batch_schedule="1d",
    ttl="3650days",
    family='fraud',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    description="User date of birth, entered at signup.",
)
def user_date_of_birth(fraud_users):
    from pyspark.sql import functions as f
    return fraud_users \
        .withColumn("user_date_of_birth", f.date_format(f.col("dob"), "yyyy-MM-dd")) \
        .withColumnRenamed("signup_date", "timestamp") \
        .select("user_id", "user_date_of_birth", "timestamp")
