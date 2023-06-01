import json
from sqlite3 import ProgrammingError

import numpy as np
import pandas
import pandas as pd
import requests
import shapely
from dotenv import dotenv_values
from geopandas import GeoDataFrame as gpd, GeoDataFrame, points_from_xy, GeoSeries
from pandas import Series, DataFrame
from scipy.spatial import cKDTree
from shapely import wkt
from shapely.geometry import Point
from sqlalchemy import create_engine
from date_range import DateRange
from exceptions import InvalidLocationIdList

frost_station_endpoint = 'https://frost.met.no/sources/v0.jsonld'
frost_observation_endpoint = 'https://frost.met.no/observations/v0.jsonld'
secrets = dotenv_values('.env')


def get_station_observations(ids: str, date_range: DateRange) \
        -> list[dict]:
    """
    This function gets the historical precipitation measured by the requested weather stations

    :param ids: List of weather station ids
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: returns a list of dictionaries, where a dictionary contains a historic precipitation observation.
    """
    print(ids)
    parameters = {
        'sources': [ids],
        'elements': 'sum(precipitation_amount PT1H)',
        'referencetime': date_range,
        'fields': 'geometry, value, referenceTime, sourceId',
    }
    try:
        r = requests.get(frost_observation_endpoint, parameters, auth=(secrets['MET_FROST_CLIENT_ID'], ''))
        print(r)
        if r.status_code == 200:
            response = r.json()
            df = pd.json_normalize(response['data'])
            df.rename(columns={'sourceId': 'id'}, inplace=True)
            df['id'] = [i[:7] for i in df.id]
            df['precipitation'] = [[i[0]['value']] for i in df['observations']]
            agg_functions = {'id': 'first', 'precipitation': 'sum'}
            dff = df.groupby(df.id).aggregate(agg_functions).reset_index(drop=True)
            dfff = dff[dff['precipitation'].apply(lambda x: len(x) == len(date_range.unix_list))]
            return dfff
        if r.status_code == 412:
            json = r.json()
            data = 'FROST API (Observations): ' + json['error']['reason']
            return data
        if r.status_code == 404:
            json = r.json()
            data = 'FROST API (Observations): ' + json['error']['reason']
            return data
        if r.status_code == 400:
            json = r.json()
            data = 'FROST API (Observations): ' + json['error']['reason']
            return data
    except Exception as e:
        print(f"Something is really wrong :( ({e})")


def get_nearest_stations_to_point(point: Series, date_range: DateRange, number_of_nearest_stations: int) \
        -> pandas.DataFrame:
    """
    Gets a defined number of weather stations that are close to the selected point on the map.

    :param point: A row of the geodataframe containing the information of one point.
    :param date_range: The date range of which the user wants the historic precipitation.
    :param number_of_nearest_stations: The number of weather stations near the selected point.
    :return: list containing a defined number of weather stations
    """
    parameters = {
        'types': 'SensorSystem',
        'elements': 'sum(precipitation_amount PT1H)',
        'nearestmaxcount': number_of_nearest_stations,
        'geometry': f'nearest({point[0]})',
        'fields': 'geometry, distance, id, name',
        'validtime': date_range
    }
    r = requests.get(frost_station_endpoint, parameters, auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    if r.status_code == 200:
        response = r.json()
        df = pd.json_normalize(response['data'], max_level=0)
        df['geometry'] = df.apply(lambda row: Point(row['geometry']['coordinates']), axis=1)
        return df


def get_processed_station_observations(point: Series, date_range: DateRange) \
        -> DataFrame:
    """
    loops through all the weather stations with a complete observation list and returns the closest station to the
    point.

    :param point: A row of the geodataframe containing the information of one point.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: A dictionary representin the closest weather station with a complete observation list to the point
    """
    possible_stations_of_feature = get_nearest_stations_to_point(point, date_range, 50)
    observations = get_station_observations(",".join(possible_stations_of_feature.id), date_range)
    closest_stations_with_full_result_range = pd.merge(possible_stations_of_feature, observations, on=["id"])
    return closest_stations_with_full_result_range


def get_processed_station_observations_poly(point: Series, date_range: DateRange) \
        -> DataFrame:
    """
    loops through all the weather stations with a complete observation list and returns the closest station to the
    point.

    :param point: A row of the geodataframe containing the information of one point.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: A dictionary representin the closest weather station with a complete observation list to the point
    """
    possible_stations_of_feature = get_station_within_polygon(point, date_range)
    observations = get_station_observations(",".join(possible_stations_of_feature.id), date_range)
    closest_stations_with_full_result_range = pd.merge(possible_stations_of_feature, observations, on=["id"])
    return closest_stations_with_full_result_range


def get_station_within_polygon(polygon: Series, date_range: DateRange) -> DataFrame:
    """
    Gets all the weather stations that are within the selected area on the map.

    :param polygon: A row of the geodataframe containing the information of one polygon.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: list containing all weather stations within the defined polygon.
    """
    parameters = {
        'types': 'SensorSystem',
        'elements': 'sum(precipitation_amount PT1H)',
        'geometry': polygon,
        'fields': 'geometry, distance, id, name',
        'validtime': date_range
    }
    r = requests.get(frost_station_endpoint, parameters, auth=(secrets['MET_FROST_CLIENT_ID'], ''))
    if r.status_code == 404:
        json = r.json()
        print('FROST API (Stations): ' + json['error']['reason'])
    if r.status_code == 200:
        response = r.json()
        df = pd.json_normalize(response['data'], max_level=0)
        df['geometry'] = df.apply(lambda row: Point(row['geometry']['coordinates']), axis=1)
        return df


def get_geo_dataframe(location_ids: list[str], validation=None) -> gpd | None:
    """
    Prepares a GeoDataFrame containing the desired GeoFeatures using prepared SQL query.

    :param validation:
    :param location_ids:
    :param engine: SqlAlchemy Engine object that allows you to connect to the desired 7A PostGIS tables.
    :return: GeoDataFrame containing the desired GeoFeatures.
    """
    sql = f"""SELECT * FROM smart_culverts_tim where providerid in ({(", ".join(location_ids))})"""
    if validation:
        _ = pd.DataFrame(gpd.GeoDataFrame.from_postgis(sql, create_engine(
            "postgresql://postgres:BergenFlom2020!@dev.7analytics.no:5432/postgres"), geom_col='geometry'))
        return None
    try:
        dataframe = pd.DataFrame(gpd.GeoDataFrame.from_postgis(sql, create_engine(
            "postgresql://postgres:BergenFlom2020!@dev.7analytics.no:5432/postgres"), geom_col='geometry'))
        print(dataframe)
    except ProgrammingError:
        raise InvalidLocationIdList(location_ids)

    dataframe['geometry'] = dataframe['geom_pol'].apply(lambda x: shapely.wkt.loads(x))
    return gpd(data=dataframe, geometry=dataframe['geometry'], crs=25833).to_crs(crs=4326)


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
