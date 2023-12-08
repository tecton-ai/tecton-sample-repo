from datetime import *
import tecton
import pandas

entity = tecton.Entity(name="user", join_keys=["USER_ID"])

schema = [
    tecton.types.Field("USER_ID", tecton.types.Int64),
    tecton.types.Field("TIMESTAMP", tecton.types.Timestamp),
    tecton.types.Field("VALUE", tecton.types.Int64),
]

start = datetime(2023,10,20,0,0,0)
end = datetime(2023,10,23,0,0,0)

@tecton.pandas_batch_config()
def backfill_data():
    start = datetime(2023,10,20,0,0,0)
    end = datetime(2023,10,23,0,0,0)
    return pandas.DataFrame({
        "USER_ID": [1,2,3],
        "TIMESTAMP": [
            start + timedelta(days=1),
            start + timedelta(days=1, hours=1),
            start + timedelta(days=1, hours=2),
        ],
        "VALUE": [1,2,3],
        })


source = tecton.PushSource(
        name = "s",
        schema = schema,
        batch_config=backfill_data,
)


bfv = tecton.StreamFeatureView(
    name="bfv",
    mode="pandas",
    source=source,
    entities=[entity],
    batch_schedule=timedelta(days=1),
    feature_start_time=start,
    manual_trigger_backfill_end_time=end,
    timestamp_field="TIMESTAMP",
    offline=True,
    online=True,
    offline_store=tecton.DeltaConfig(),
    ttl=timedelta(days=365),
    schema=schema,
)


bfv_feature_service = FeatureService(
    name='bfv_feature_service',
    description='A FeatureService of a simple backfill feature',
    tags={'release': 'production'},
    owner='meastham@tecton.ai',
    online_serving_enabled=True,
    features=[
        bfv,
    ],
)
