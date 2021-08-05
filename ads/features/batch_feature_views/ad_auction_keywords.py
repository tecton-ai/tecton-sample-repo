from tecton import transformation, Input, batch_feature_view, const, BackfillConfig
from ads.entities import auction
from ads.data_sources.ad_impressions import ad_impressions_batch
from datetime import datetime

# Create new column by splitting the string in an existing column.
@transformation(mode="spark_sql")
def str_split(input_data, column_to_split, new_column_name, delimiter):
    return f"""
    SELECT
        *,
        split({column_to_split}, {delimiter}) AS {new_column_name}
    FROM {input_data}
    """

# Create features based on the keyword array
@transformation(mode="spark_sql")
def keyword_stats(input_data, keyword_column):
    return f"""
    SELECT
        auction_id,
        timestamp,
        size({keyword_column}) AS num_keywords,
        concat_ws(",", {keyword_column}) AS keyword_list,
        array_contains({keyword_column}, "bitcoin") AS keyword_contains_bitcoin
    FROM {input_data}
    """

# This feature view runs in pipeline mode to turn the keyword string into an
# array of words, then create metrics based on that array.
@batch_feature_view(
    mode='pipeline',
    inputs={
        'ad_impressions': Input(ad_impressions_batch)
    },
    entities=[auction],
    ttl='1d',
    batch_schedule='1d',
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 5, 1),
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    family='ads',
    owner='derek@tecton.ai',
    tags={'release': 'production'}
    )
def auction_keywords(ad_impressions):
    split_keywords = str_split(ad_impressions, const("content_keyword"), const("keywords"), const("\' \'"))
    return keyword_stats(split_keywords, const("keywords"))
