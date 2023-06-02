# from datetime import timedelta, datetime
# from tecton import StreamFeatureView, FilteredSource, stream_feature_view, BatchTriggerType, Aggregation, Entity, PushSource
# from tecton.types import Field, String, Timestamp, Int64, Float64
# from ads.entities import content_keyword, user
#
#
# input_schema = [
#     Field(name="lat", dtype=Float64),
#     Field(name="long", dtype=Float64),
#     Field(name="timestamp", dtype=Timestamp),
#     Field(name="current_temperature", dtype=Float64)
# ]
#
#
# output_schema = [
#     Field(name="lat", dtype=Float64),
#     Field(name="long", dtype=Float64),
#     Field(name="timestamp", dtype=Timestamp),
#     Field(name="current_temperature", dtype=Float64)
# ]
#
# location = Entity(
#     name='location',
#     join_keys=['lat','long'],
#     description='A location',
#     owner='t-rex@tecton.ai',
#     tags={'release': 'production'}
# )
#
# location_source = PushSource(
#     name="location_source",
#     schema=input_schema,
#     description="A push source for synchronous, online ingestion of ad-click events with user info.",
#     owner="pooja@tecton.ai",
#     tags={'release': 'staging'}
# )
#
#
# @stream_feature_view(
#     name="weather_features",
#     source=location_source,
#     entities=[location],
#     online=True,
#     offline=True,
#     feature_start_time=datetime(2023, 1, 1),
#     batch_schedule=None,
#     ttl=timedelta(days=30),
#     tags={'release': 'production'},
#     owner='achal@tecton.ai',
#     description='The ad clicks for a user',
#     mode='python',
#     schema=output_schema,
#     batch_trigger=BatchTriggerType.NO_BATCH_MATERIALIZATION,
# )
# def weather_features(location_source):
#     import http.client
#     h1 = http.client.HTTPConnection("api.weather.gov")
#     # f_resp = requests.get(f"https://api.weather.gov/points/{location_source['lat']},{location_source['long']}")
#     # weather_details = f_resp.json()['properties']['forecast']
#     # current_weather = requests.get(weather_details)
#     # temp = current_weather.json()['properties']['periods'][0]['temperature']
#     # location_source['current_temperature'] = float(temp)
#     return location_source
