from tecton import batch_feature_view, Input, BackfillConfig, MonitoringConfig
from fraud.entities import user
from fraud.data_sources.fraud_users_batch import fraud_users_batch
from datetime import datetime


@batch_feature_view(
    inputs={'users': Input(fraud_users_batch)},
    entities=[user],
    mode='pyspark',
    online=True,
    offline=True,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017,1, 1),
    batch_schedule='1d',
    ttl='3650days',
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    monitoring=MonitoringConfig(alert_email="derek@tecton.ai", monitor_freshness=False),
    family='fraud',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='User date of birth, entered at signup.',
)
def user_date_of_birth(users):
    from pyspark.sql import functions as f
    return users \
        .withColumn('user_date_of_birth', f.date_format(f.col('dob'), 'yyyy-MM-dd')) \
        .withColumnRenamed('signup_date', 'timestamp') \
        .select('user_id', 'user_date_of_birth', 'timestamp')
