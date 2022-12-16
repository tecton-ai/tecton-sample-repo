from tecton import batch_feature_view
import datetime
import data_sources
import entities

@batch_feature_view(
    sources=[data_sources.dummy_batch_source],
    entities=[entities.user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime.datetime(2022, 12, 1),
    batch_schedule=datetime.timedelta(days=1),
    ttl=datetime.timedelta(days=30),
)
def bfv_materialization_disabled(source):
    return f'SELECT * FROM {source}'

@batch_feature_view(
    sources=[data_sources.dummy_batch_source],
    entities=[entities.user],
    mode='spark_sql',
    online=False,
    offline=True,
    feature_start_time=datetime.datetime(2022, 12, 1),
    batch_schedule=datetime.timedelta(days=1),
    ttl=datetime.timedelta(days=30),
)
def bfv_materialization_enabled(source):
    return f'SELECT * FROM {source}'

@batch_feature_view(
    sources=[data_sources.dummy_batch_source],
    entities=[entities.user],
    mode='spark_sql',
    online=False,
    offline=False,
    incremental_backfills=True,
    feature_start_time=datetime.datetime(2022, 12, 1),
    batch_schedule=datetime.timedelta(days=1),
    ttl=datetime.timedelta(days=30),
)
def bfv_incremental_backfill_with_materialization_disabled(source):
    return f'SELECT * FROM {source}'

@batch_feature_view(
    sources=[data_sources.dummy_batch_source],
    entities=[entities.user],
    mode='spark_sql',
    online=False,
    offline=True,
    incremental_backfills=True,
    feature_start_time=datetime.datetime(2022, 12, 1),
    batch_schedule=datetime.timedelta(days=1),
    ttl=datetime.timedelta(days=30),
)
def bfv_incremental_backfill_with_materialization_enabled(source):
    return f'SELECT * FROM {source}'
