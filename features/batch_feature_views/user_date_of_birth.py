from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.users import users
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(users)],
    entities=[user],
    mode='pyspark',
    online=True,
    offline=True,
    feature_start_time=datetime(2017,1, 1),
    schedule_interval=timedelta(days=1),
    owner='derek@tecton.ai',
    tags={'release': 'production'},
    description='User date of birth, entered at signup.',
    ttl=timedelta(days=10 * 365),
)
def user_date_of_birth(users):
    from pyspark.sql import functions as f
    return users \
        .withColumn('user_date_of_birth', f.date_format(f.col('dob'), 'yyyy-MM-dd')) \
        .withColumnRenamed('signup_date', 'timestamp') \
        .select('user_id', 'user_date_of_birth', 'timestamp')
