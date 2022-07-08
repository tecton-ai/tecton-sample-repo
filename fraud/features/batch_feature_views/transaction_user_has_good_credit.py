from tecton.compat import batch_feature_view, Input, BackfillConfig
from fraud.entities import user
from fraud.data_sources.credit_scores_batch import credit_scores_batch
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime


# For every transaction, the following FeatureView precomputes a feature that indicates
# whether a user had a credit score > 650 as of the time of the transaction
@batch_feature_view(
    inputs={'credit_scores': Input(credit_scores_batch, window="730d"),
            'transactions': Input(transactions_batch)},
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule='1d',
    ttl='120d',
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    family='fraud',
    tags={'release': 'production'},
    owner='kevin@tecton.ai',
    description='Whether the user had a good credit score (over 670) as of the time of a transaction.'
)
def transaction_user_has_good_credit(credit_scores, transactions):
    return f'''
        select transaction_timestamp as timestamp, user_id, IF (credit_score > 670, 1, 0) as user_has_good_credit from 
        (
          select *, 
            row_number() over (partition by user_id, transaction_timestamp order by score_timestamp desc) as row_num
            from 
              (
                select t.timestamp as transaction_timestamp, s.timestamp as score_timestamp, user_id, amount, credit_score 
                  from {transactions} t 
                  join {credit_scores} s on t.nameorig=s.user_id and s.timestamp <= t.timestamp
              )
        )
        where row_num = 1
    '''
