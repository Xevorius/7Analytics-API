import json
from typing import Tuple, Callable, Any

import geopandas
import numpy as np
import pandas as pd
import requests
from dotenv import dotenv_values
from geopandas import GeoDataFrame, GeoSeries, points_from_xy
from pandas import DataFrame
from scipy.spatial import cKDTree

from date_range import DateRange

secrets = dotenv_values('.env')


def get_waterlevel_flow_observations(ids: str, date_range: DateRange):
    parameters = {
        'StationId': ids,
        'Parameter': '1000,1001',
        'ResolutionTime': 60,
        'ReferenceTime': date_range
    }
    url = f"https://hydapi.nve.no/api/v1/Observations"
    request_headers = {
        "Accept": "application/json",
        "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    }
    nve_stations = requests.get(url, parameters, headers=request_headers)
    parsed_result = nve_stations.json()
    df = pd.json_normalize(parsed_result['data'])
    return df


def get_nearest_station_obesrvation(shape: GeoDataFrame, date_range: DateRange):
    url = f'https://hydapi.nve.no/api/v1/Stations?Active=1'
    request_headers = {
        "Accept": "application/json",
        "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    }
    nve_stations = requests.get(url, headers=request_headers)
    parsed_result = nve_stations.json()
    df = pd.json_normalize(parsed_result['data'])
    df['parameters'] = [[x['parameter'] for x in i] for i in df.seriesList]
    df = df[df['parameters'].apply(lambda x: 1000 in x or 1001 in x)]
    df['seriesList'] = [[x for x in i if x['parameter'] == 1000 or x['parameter'] == 1001] for i in df.seriesList]
    df['dateRange'] = [[[DateRange(f"{y['dataFromTime'][:10]}/{y['dataToTime'][:10]}") for y in x['resolutionList'] if
                         y['resTime'] == 60] for x in i] for i in df.seriesList]
    df = df[df['dateRange'].apply(lambda x: bool(list(filter(None, x))))]
    gdf = geopandas.GeoDataFrame(
        df[["stationId", "stationName", "latitude", "longitude", "seriesList"]],
        geometry=geopandas.points_from_xy(df.longitude, df.latitude), crs=4326)
    culvert = shape.sjoin_nearest(gdf).merge(gdf, left_on="index_right", right_index=True)
    culvert_id = culvert.iloc[0]['stationId_x']
    observations = get_waterlevel_flow_observations(culvert_id, date_range)
    return observations
