from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.credit_scores import credit_scores
from data_sources.transactions_batch import transactions_batch
from datetime import datetime, timedelta


@batch_feature_view(
    source=[FilteredSource(credit_scores), FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule=timedelta(days=1),
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Whether the user had a good credit score (over 670) as of the time of a transaction.'
)
def transaction_user_has_good_credit(credit_scores, transactions_batch):
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
