import io
import itertools
import time
from typing import Any

import geopandas
import pandas as pd
import requests
import shapely
import tiledb
from fastapi import FastAPI
from geopandas import GeoSeries
import numpy as np
from pyogrio import read_dataframe
from sqlalchemy import create_engine
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from MET.forecast_api import get_forecast
from PipeLife.culvert import PipeLifeUser
from date_range import DateRange
from MET.met_api import get_nearest_stations_to_point, get_weather_stations, get_station_within_polygon, \
    get_station_observations, get_stations_with_full_observations

tags_metadata = [
    {
        "name": "Default",
        "description": "All operations without any input variables",
    },
    {
        "name": "MET.NO",
        "description": "All operations interfacing with **MET.NO** data gathered from the FROST API.",
    },
    {
        "name": "IMERG",
        "description": "All operations interfacing with **IMERG** data.",
    },
    {
        "name": "NVE",
        "description": "All operations interfacing with **NVE** data gathered from the NVE API.",
    },
    {
        "name": "PipeLife",
        "description": "All operations interfacing with **PipeLife** data gathered from the PipeLife API.",
    },
]

app = FastAPI(title="Smart Culvert API", version="0.1.3", openapi_tags=tags_metadata, docs_url="/")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

options = {'vfs.s3.aws_access_key_id': '',
           'vfs.s3.aws_secret_access_key': '',
           'vfs.s3.scheme': 'https',
           'vfs.s3.region': 'eu-west-1'
           }

context = tiledb.Config(options)

"""
Uncomment the following to line if you intend to use the IMERG interface. Keep in mind that it will take a considerable
amount of time to load these variables.
"""
# tbarray = tiledb.open(f'', mode="r", config=context)
# tbarray_dask = da.from_tiledb(f'',
#                          storage_options=options, attribute='precipitationCal')

"""
Uncomment the following line if you want to monitoring the Dask calculations though a  Dask diagnostics panel.
"""
# client = Client("tcp://127.0.0.1:63883")


@app.get('/', tags=['Default'])
def index() -> str:
    return 'Welcome to the historical IMERG precipitation api'


@app.get('/pour_point', tags=['Default'])
def get_pour_point_feature_collection() -> str:
    """
    Returns all the pour points from the POSTGIS database in a GeoJSON format.
    """
    sql = f"""SELECT * FROM smartculvert.pour_point"""
    con = create_engine("")
    dataframe = geopandas.GeoDataFrame.from_postgis(sql, con)
    gdf = dataframe.to_crs(4326)

    print(gdf.to_json())
    return gdf.to_json()


@app.get("/MET/point/nearest", tags=['MET.NO'])
def get_nearest_weather_station(point_wkt: str, date_range: str, crs=4326) -> list[dict]:
    """
    Returns the MET station id closest to the input point with complete precipitation data.
    """
    date_range = DateRange(date_range)
    point = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    station = get_nearest_stations_to_point(point, date_range, 1)
    return station


@app.get("/MET/point/precipitation", tags=['MET.NO'])
async def get_nearest_full_weather_station(point_wkt: str, date_range: str, file=None,
                                           crs=4326):
    """
    Returns a **list** or **.CSV** containing the hourly precipitation measured by the weather station closed to the
    input point with complete precipitation data.
    """
    date_range = DateRange(date_range)
    point = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    station_precipitation = get_weather_stations(point, date_range)

    if file:
        print(len(date_range.unix_list))
        data = pd.DataFrame(
            {'timestamp': date_range.unix_list,
             'precipitation': station_precipitation['value_list'],
             })
        stream = io.StringIO()
        data.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]),
                                     media_type="text/csv"
                                     )
        response.headers["Content-Disposition"] = f"attachment; filename={station_precipitation['name']}.csv"
        return response

    else:
        return station_precipitation


@app.get("/IMERG/point/precipitation", tags=['IMERG'])
async def get_imerg_precipitation_from_point(point_wkt: str, date_range: str, file=False, crs=4326, dask=False):
    """
    Access the full historical IMERG precipitation (2000-06-01/2021-09-31). Keep in mind that this dataset is not fully
    optimized yet. Thus, the more data you request the longer your request will take.
    (1 day = 1 second~ | 1 year = 130 seconds~)

    :param point_wkt: String representation of the wkt point you want to get the precipitation from.
    :param date_range: Define the temporal range of the data you want. Format: YYYY-MM-DD/YYYY-MM-DD.
    :param file: (default = False) If true, the output will be a .csv file instead of a list.
    :param crs: (default = 4326) Define the CRS your wkt point is in.
    :param dask: (default = False) Not implemented yet!
    :return:
    """
    date_range = DateRange(date_range)
    point = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    df_with_geometries = pd.concat([point, point.bounds], axis=1)
    df_with_geometries.rename(columns={df_with_geometries.columns[0]: 'geometry'}, inplace=True)
    df_with_geometries['sub_geom'] = df_with_geometries.geometry
    grid_geometries = read_dataframe('IMERG/Grid.fgb', bbox=(
        df_with_geometries['minx'], df_with_geometries['miny'], df_with_geometries['maxx'], df_with_geometries['maxy']))
    gdf = geopandas.GeoDataFrame(grid_geometries).drop_duplicates('label')
    joined_df = gdf.sjoin(geopandas.GeoDataFrame(
        df_with_geometries.drop(['minx', 'miny', 'maxx', 'maxy'], axis=1),
        geometry=df_with_geometries.geometry))
    joined_df["percent_coverage"] = 1
    max_interval_list = [t for t in
                         np.arange(date_range.min_date,
                                   date_range.max_date + np.timedelta64(30, 'm'),
                                   np.timedelta64(30, "m"))]
    max_interval_list.index(date_range.min_date), max_interval_list.index(date_range.max_date)
    date_index = max_interval_list.index(date_range.min_date), max_interval_list.index(date_range.max_date)
    joined_df = joined_df.iloc[0]
    index_list = [int(i) for i in joined_df['label'].split(',')]
    start_time = time.time()
    if dask:
        print("dask")
        result = tbarray_dask[date_index[0]:date_index[1] + 1, index_list[1]:index_list[1] + 1,
                 index_list[0]:index_list[0] + 1]
        reshaped = np.nan_to_num(result.reshape(int(len(result))).compute(), nan=-1)
    else:
        print("numpy")
        result = np.array(tbarray[date_index[0]:date_index[1] + 1, index_list[1]:index_list[1] + 1,
                          index_list[0]:index_list[0] + 1]['precipitationCal'])
        reshaped = np.nan_to_num(result.reshape(int(result.size)), nan=-1)

    results = [np.sum(i) for i in reshaped.reshape(int(len(reshaped) / 2), 2).round(5)]
    print('Full calculation took: ', time.time() - start_time, ' seconds.')
    if file:
        data = pd.DataFrame(
            {'timestamp': date_range.unix_list,
             'precipitation': results,
             })
        stream = io.StringIO()
        data.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]),
                                     media_type="text/csv"
                                     )
        response.headers["Content-Disposition"] = f"attachment; filename={'IMERG'}.csv"
        return response

    else:
        return results


@app.get("/MET/polygon/all", tags=['MET.NO'])
def get_all_weather_station(polygon_wkt: str, date_range: str, crs=4326) -> list[dict]:
    """
    Get all weather stations within the input polygon.
    """
    date_range = DateRange(date_range)
    polygon = GeoSeries.from_wkt([polygon_wkt], crs=crs).to_crs(4326)
    stations = get_station_within_polygon(polygon, date_range)
    return stations


@app.get("/MET/polygon/precipitation", tags=['MET.NO'])
def get_all_weather_station_precipitation(polygon_wkt: str, date_range: str, crs=4326) -> list[dict]:
    """
    Returns a list of the hourly precipitation of all weather stations within the input polygon with full precipitation.
    """
    date_range = DateRange(date_range)
    polygon = GeoSeries.from_wkt([polygon_wkt], crs=crs).to_crs(4326)
    stations = get_station_within_polygon(polygon, date_range)
    observations = get_station_observations(','.join(str(station['id']) for station in stations),
                                            date_range)
    closest_stations_with_full_result_range = get_stations_with_full_observations(stations, observations, date_range)
    if len(closest_stations_with_full_result_range) > 0:
        stations_with_full_result_range = [{'id': i['id'], 'name': i['name'], 'coords': i['geometry']['coordinates'],
                                            'value_list': [x['observations'][0]['value'] for x in observations if
                                                           x['sourceId'].split(':')[0] ==
                                                           i['id']]} for i in closest_stations_with_full_result_range]
    else:
        stations_with_full_result_range = {'id': [], 'name': [], 'coords': [], 'distance': [], 'value_list': []}

    return stations_with_full_result_range


@app.get("/IMERG/polygon/precipitation", tags=['IMERG'])
def get_imerg_precipitation_from_polygon(polygon_wkt: str, date_range: str, crs=4326) -> list[dict]:
    """
    (_Not implemented yet!_)

    :param crs: /
    :param polygon_wkt: /
    :param date_range: /
    :return: /
    """
    date_range = DateRange(date_range)
    polygon = GeoSeries.from_wkt([polygon_wkt], crs=crs).to_crs(4326)
    return [{'Warning': 'Not implemented yet!'}]


@app.get("/NVE/point/flow", tags=['NVE'])
def get_closest_culvert_data(point_wkt: str, date_range: str, crs=4326) -> list[Any]:
    """
    Returns hourly water level and/or water flow (depending on what is available) of the closest culvert to the input
    point.
    """
    date_range = DateRange(date_range)
    pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(point_wkt)], crs=crs).to_crs(4326)

    url = f'https://hydapi.nve.no/api/v1/Stations?Active=1'
    request_headers = {
        "Accept": "application/json",
        "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",

    }
    nve_stations = requests.get(url, headers=request_headers)
    parsed_result = nve_stations.json()
    df = pd.json_normalize(parsed_result['data'])
    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude), crs=4326)
    culvert = pointer.sjoin_nearest(gdf).merge(gdf, left_on="index_right", right_index=True)
    culvert_id = culvert.iloc[0]['stationId_x']
    url = f"https://hydapi.nve.no/api/v1/Observations?StationId={culvert_id}&Parameter=1000,1001&ResolutionTime=60&ReferenceTime={date_range}"
    request_headers = {
        "Accept": "application/json",
        "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    }
    nve_stations = requests.get(url, headers=request_headers)
    return [nve_stations.json()['data'][0]['parameterNameEng'], nve_stations.json()['data'][0]['observations']]


@app.get("/NVE/polygon/flow", tags=['NVE'])
def get_all_culvert_data_within_polygon(polygon_wkt: str, date_range: str, crs=4326) -> list[Any]:
    """
    **Not implemented yet**
    """
    pass
    # date_range = DateRange(date_range)
    # pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(polygon_wkt)], crs=crs).to_crs(4326)
    #
    # url = f'https://hydapi.nve.no/api/v1/Stations?Active=1'
    # request_headers = {
    #     "Accept": "application/json",
    #     "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    #
    # }
    # nve_stations = requests.get(url, headers=request_headers)
    # parsed_result = nve_stations.json()
    # df = pd.json_normalize(parsed_result['data'])
    # gdf = geopandas.GeoDataFrame(
    #     df, geometry=geopandas.points_from_xy(df.longitude, df.latitude), crs=4326)
    # culvert = pointer.sjoin_nearest(gdf).merge(gdf, left_on="index_right", right_index=True)
    # culvert_id = culvert.iloc[0]['stationId_x']
    # url = f"https://hydapi.nve.no/api/v1/Observations?StationId={culvert_id}&Parameter=1000,1001&ResolutionTime=60&ReferenceTime={date_range}"
    # request_headers = {
    #     "Accept": "application/json",
    #     "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    # }
    # nve_stations = requests.get(url, headers=request_headers)
    # return [nve_stations.json()['data'][0]['parameterNameEng'], nve_stations.json()['data'][0]['observations']]


@app.get("/PipeLife/id/waterlevel", tags=['PipeLife'])
def get_water_level_from_id(pipelife_ids: str, date_range: str) -> list[Any]:
    """
    Returns a list containing hourly water level data of the input culvert.
    """
    date_range = DateRange(date_range)

    pipelife_users = []

    pipelifeusers = [PipeLifeUser(client_id=user['TCN_CLIENT_ID'], client_secret=user['TCN_CLIENT_SECRET'],
                                  client_username=user['TCN_MYUSER'], password=user['TCN_MYPASS']) for user in
                     pipelife_users]
    selected_culverts = list(itertools.chain(
        *[x.get_culverts_from_id_list(pipelife_ids.split(','), date_range) for x in pipelifeusers]))
    all_pipes_data = [[culvert._location_id, culvert.get_hourly_data()] for culvert in selected_culverts]
    return all_pipes_data


@app.get("/MET/point/forecast", tags=['MET.NO'])
def get_forecast_from_point(point_wkt: str, crs=4326) -> list[Any]:
    """
    Returns a list containing the forecasted precipitation of the input point.
    """
    pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(point_wkt)], crs=crs).to_crs(4326)

    forecast = get_forecast(pointer)
    return forecast
