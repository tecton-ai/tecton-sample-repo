from tecton import transformation, FilteredSource, batch_feature_view, const, Attribute
from tecton.types import String, Array, Int32, Bool
from ads.entities import auction
from ads.data_sources.ad_impressions import ad_impressions_batch
from datetime import datetime, timedelta

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
        {keyword_column} AS keyword_list,
        size({keyword_column}) AS num_keywords,
        array_contains({keyword_column}, "bitcoin") AS keyword_contains_bitcoin
    FROM {input_data}
    """

# This feature view runs in pipeline mode to turn the keyword string into an
# array of words, then create metrics based on that array.
@batch_feature_view(
    mode='pipeline',
    sources=[FilteredSource(ad_impressions_batch)],
    entities=[auction],
    timestamp_field="timestamp",
    features=[
        Attribute(name="auction_id", dtype=String),
        Attribute(name="keyword_list", dtype=Array(String)),
        Attribute(name="num_keywords", dtype=Int32),
        Attribute(name="keyword_contains_bitcoin", dtype=Bool),
    ],
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
    )
def auction_keywords(ad_impressions):
    split_keywords = str_split(ad_impressions, const("content_keyword"), const("keywords"), const("\' \'"))
    return keyword_stats(split_keywords, const("keywords"))
