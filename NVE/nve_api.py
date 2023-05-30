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
    url = f"https://hydapi.nve.no/api/v1/Observations?StationId={ids}&Parameter=1000,1001&ResolutionTime=60&ReferenceTime={date_range}"
    request_headers = {
        "Accept": "application/json",
        "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    }
    nve_stations = requests.get(url, headers=request_headers)
    parsed_result = nve_stations.json()
    df2 = pd.json_normalize(parsed_result['data'])
    return df2


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
    print(gdf)
    culvert = shape.sjoin_nearest(gdf).merge(gdf, left_on="index_right", right_index=True)
    culvert_id = culvert.iloc[0]['stationId_x']
    observations = get_waterlevel_flow_observations(culvert_id, date_range)
    print(observations)


def get_idf_from_raster(pointer: GeoSeries):
    parameters = {
        'sources': 'idf_bma1km',
        'location': pointer.geometry,
        'unit': 'l/s*Ha'
    }
    r = requests.get('https://frost.met.no/frequencies/rainfall/v0.jsonld', parameters,
                     auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    response = r.json()
    parameters['unit'] = 'mm'
    r = requests.get('https://frost.met.no/frequencies/rainfall/v0.jsonld', parameters,
                     auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    response2 = r.json()
    df = pd.json_normalize(response['data'][0]['values'], max_level=0)
    df['intensity'] = df['intensity'].div(10000)
    df_pivoted = df.pivot(index='frequency', columns='duration', values='intensity')

    dfmm = pd.json_normalize(response2['data'][0]['values'], max_level=0)
    dfmm_pivoted = dfmm.pivot(index='frequency', columns='duration', values='intensity')
    return json.dumps({'l/s*Ha': df_pivoted.to_dict(), 'mm': dfmm_pivoted.to_dict()})

def get_idf_curve_from_nearest_station(row: GeoSeries) -> str:
    """
    Generate the IDF curves for the given location from the nearest station in both liter per m2 and millimeter.

    :param stations:
    :param row: GeoSeries containing a point of which you want the IDF curves.
    :return: A tuple containing the IDF curves as DataFrames.
    """

    nearest_station = get_closest_idf_station_id(row, load_idf_stations())
    parameters = {
        'sources': nearest_station['id'],
        'unit': 'l/s*Ha'
    }
    r = requests.get('https://frost.met.no/frequencies/rainfall/v0.jsonld', parameters,
                     auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    response = r.json()
    parameters['unit'] = 'mm'
    r = requests.get('https://frost.met.no/frequencies/rainfall/v0.jsonld', parameters,
                     auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    response2 = r.json()
    df = pd.json_normalize(response['data'][0]['values'], max_level=0)
    df['intensity'] = df['intensity'].div(10000)
    df_pivoted = df.pivot(index='frequency', columns='duration', values='intensity')

    dfmm = pd.json_normalize(response2['data'][0]['values'], max_level=0)
    dfmm_pivoted = dfmm.pivot(index='frequency', columns='duration', values='intensity')

    return json.dumps({'l/s*Ha': df_pivoted.to_dict(), 'mm': dfmm_pivoted.to_dict(), 'station': nearest_station['id']})


def get_closest_idf_station_id(row: GeoSeries, stations: GeoDataFrame) -> GeoSeries:
    """


    :param row:
    :param stations:
    :return:
    """
    # point = GeoDataFrame(geometry=[row.geometry])
    n_a = np.array(list(row.geometry.apply(lambda x: (x.x, x.y))))
    n_b = np.array(list(stations.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(n_b)
    dist, idx = btree.query(n_a, k=1)
    gdb_nearest = stations.iloc[idx].drop(columns="geometry").reset_index(drop=True)
    gdf = pd.concat(
        [
            row.reset_index(drop=True),
            gdb_nearest,
            pd.Series(dist, name='dist')
        ],
        axis=1)
    print(type(gdf.iloc[0]))
    return gdf.iloc[0]


def load_idf_stations() -> GeoDataFrame:
    """
    Gathers all the IDF stations by querying the MET API.

    :return: GeoDataFrame with all available MET IDF stations.
    """
    r = requests.get('https://frost.met.no/frequencies/rainfall/availableSources/v0.jsonld', {},
                     auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    response = r.json()
    df = pd.json_normalize(response['data'], max_level=0)
    df = df.rename(columns={"sourceId": "id"})
    df = df[df.id != 'idf_bma1km']
    parameters = {
        'types': 'SensorSystem',
        'ids': ",".join(df.id),
        'fields': 'geometry, id'
    }
    r = requests.get('https://frost.met.no/sources/v0.jsonld?', parameters, auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    response = r.json()
    df2 = pd.json_normalize(response['data'], max_level=0)
    df3 = pd.merge(df, df2, on=["id"])
    df3['x'] = [i['coordinates'][0] for i in df3['geometry']]
    df3['y'] = [i['coordinates'][1] for i in df3['geometry']]
    return GeoDataFrame(df3, geometry=points_from_xy(df3['x'], df3['y']))
