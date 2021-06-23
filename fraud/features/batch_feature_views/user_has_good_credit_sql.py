from tecton import batch_feature_view, Input
from fraud.entities import user
from fraud.data_sources.credit_scores_batch import credit_scores_batch
from datetime import datetime


@batch_feature_view(
    inputs={'credit_scores': Input(credit_scores_batch)},
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule='1d',
    ttl='120d',
    family='fraud',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='Whether the user has a good credit score (over 670).'
)
def user_has_good_credit_sql(credit_scores):
    return f'''
        SELECT
            user_id,
            IF (credit_score > 670, 1, 0) as user_has_good_credit,
            date as timestamp
        FROM
            {credit_scores}
        '''
