from tecton import batch_feature_view, Attribute
from tecton.types import String, Array, Int32, Bool
from ads.entities import auction
from ads.data_sources.ad_impressions import ad_impressions_batch
from datetime import datetime, timedelta


# This feature view runs in pipeline mode to turn the keyword string into an
# array of words, then create metrics based on that array.
@batch_feature_view(
    mode='pandas',
    sources=[ad_impressions_batch],
    entities=[auction],
    timestamp_field="timestamp",
    features=[
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
    df = ad_impressions.copy()
    
    # Split content_keyword into array of keywords
    df['keyword_list'] = df['content_keyword'].str.split(' ')
    
    # Calculate keyword stats
    df['num_keywords'] = df['keyword_list'].str.len()
    df['keyword_contains_bitcoin'] = df['keyword_list'].apply(lambda x: 'bitcoin' in x)
    
    # Select only the required columns
    return df[['auction_id', 'timestamp', 'keyword_list', 'num_keywords', 'keyword_contains_bitcoin']]
