from tecton.feature_views import batch_feature_view, Input
from core.entities import user
from core.data_sources.users_batch import users_batch
from datetime import datetime

@batch_feature_view(
    inputs={"users": Input(users_batch)},
    entities=[user],
    mode="pyspark",
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 1, 1),
    batch_schedule="1d",
    ttl="3650days",
    family='core',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    description="User date of birth, entered at signup.",
)
def user_date_of_birth(users):
    from pyspark.sql import functions as f
    return users \
        .withColumn("user_date_of_birth", f.date_format(f.col("dob"), "yyyy-MM-dd")) \
        .withColumnRenamed("signup_date", "timestamp") \
        .select("user_id", "user_date_of_birth", "timestamp")
