from tecton import batch_feature_view
from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[fraud_users_batch],
    entities=[user],
    mode='pyspark',
    online=False,
    offline=False,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017,1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='User date of birth, entered at signup.',
)
def user_date_of_birth(fraud_users_batch):
    from pyspark.sql import functions as f
    return fraud_users_batch \
        .withColumn('user_date_of_birth', f.date_format(f.col('dob'), 'yyyy-MM-dd')) \
        .withColumnRenamed('signup_timestamp', 'timestamp') \
        .select('user_id', 'user_date_of_birth', 'timestamp')
