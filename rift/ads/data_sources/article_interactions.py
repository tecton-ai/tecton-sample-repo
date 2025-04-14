from tecton import StreamSource, PushConfig
from tecton.types import Field, Int64, String, Timestamp

article_interactions_stream = StreamSource(
    name='article_interactions_stream',
    stream_config=PushConfig(),
    tags={
        'release': 'production',
        'source': 'mobile'
    },
    schema = [
        Field("user_uuid", dtype=String),
        Field("article_id", dtype=String),
        Field("timestamp", dtype=Timestamp)
    ]
) 