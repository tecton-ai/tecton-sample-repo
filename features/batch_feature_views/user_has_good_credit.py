from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.credit_scores import credit_scores
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[FilteredSource(credit_scores)],
    entities=[user],
    mode='pyspark',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule=timedelta(days=1),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='Whether the user has a good credit score (over 670).'
)
def user_has_good_credit_pyspark(credit_scores):
    from pyspark.sql.functions import when, col
    return credit_scores.withColumn('user_has_good_credit', when(col('credit_score') > 670, 1).otherwise(0)) \
                        .select('user_id', 'user_has_good_credit', 'timestamp')
