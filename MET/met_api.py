import json
from json import loads
from sqlite3 import ProgrammingError

import pandas as pd
import requests
import shapely
from dotenv import dotenv_values
from geopandas import GeoDataFrame as gpd
from pandas import Series
from shapely.geometry import mapping
from sqlalchemy import create_engine

from Schemas.schemas_met import MetStation
from date_range import DateRange
from exceptions import InvalidLocationIdList

frost_station_endpoint = 'https://frost.met.no/sources/v0.jsonld'
frost_observation_endpoint = 'https://frost.met.no/observations/v0.jsonld'
secrets = dotenv_values('.env')


def get_station_observations(ids: str, date_range: DateRange) -> list[dict]:
    """
    This function gets the historical precipitation measured by the requested weather stations

    :param ids: List of weather station ids
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: returns a list of dictionaries, where a dictionary contains a historic precipitation observation.
    """
    parameters = {
        'sources': [ids],
        'elements': 'sum(precipitation_amount PT1H)',
        'referencetime': date_range,
        'fields': 'geometry, value, referenceTime, sourceId',
    }
    try:
        r = requests.get(frost_observation_endpoint, parameters, auth=(secrets['MET_FROST_CLIENT_ID'], ''))
        if r.status_code == 200:
            json = r.json()
            data = json['data']
            return data
        if r.status_code == 412:
            json = r.json()
            data = 'FROST API (Observations): ' + json['error']['reason']
            print(data)
            return data
        if r.status_code == 404:
            json = r.json()
            data = 'FROST API (Observations): ' + json['error']['reason']
            print(data)
            return data
        if r.status_code == 400:
            json = r.json()
            data = 'FROST API (Observations): ' + json['error']['reason']
            print(data)
            return data
    except Exception:
        print("Something is really wrong :(")


def get_nearest_stations_to_point(point: Series, date_range: DateRange, number_of_nearest_stations: int) -> list[
    MetStation]:
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
        data = response['data']
        return [MetStation.parse_obj(i) for i in data]


def get_nearest_station_observations(point: Series, date_range: DateRange) -> MetStation | None:
    """
    loops through all the weather stations with a complete observation list and returns the closest station to the
    point.

    :param point: A row of the geodataframe containing the information of one point.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: A dictionary representin the closest weather station with a complete observation list to the point
    """
    possible_stations_of_feature = get_nearest_stations_to_point(point, date_range, 50)
    observations = get_station_observations(','.join(str(station.id) for station in possible_stations_of_feature),
                                            date_range)
    closest_stations_with_full_result_range = get_stations_with_full_observations(possible_stations_of_feature,
                                                                                  observations, date_range)
    if len(closest_stations_with_full_result_range) > 0:
        full_station = closest_stations_with_full_result_range[0]
        full_station.precip_list = list(zip(date_range.unix_list, [i['observations'][0]['value'] for i in observations if
                                    i['sourceId'].split(':')[0] ==
                                    closest_stations_with_full_result_range[0].id]))
        full_station.geometry_origin = json.loads(point.to_json())['features'][0]['geometry']
        return full_station
    else:
        return None


def get_stations_with_full_observations(stations: list, observations: list[dict], date_range: DateRange) -> list[
    MetStation]:
    """
    Returns only the weather stations that have a complete observation list.

    :param stations: List of all the weather stations that are either close to a selected point or within a selected
                     area.
    :param observations: List of precipitation values of all the station in the stations list.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: List of all the weather stations with a complete observation list from closest to furthest away.
    """
    stations_with_values = list(set([i['sourceId'].split(':')[0] for i in observations]))
    stations_with_full_result_range = [i for i in stations if i.id in stations_with_values if
                                       len([x for x in observations if
                                            x['sourceId'].split(':')[0] == i.id]) ==
                                       len(date_range.unix_list)]
    return stations_with_full_result_range


def get_weather_stations(point: Series, date_range: DateRange) -> MetStation | None:
    """
    This function gets all the historical precipitation based on the user's inputted geofeatures.

    :param feature_frame: Geodataframe containing all the user's inputted geofeatures.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: A dictionary containing all the weather station and their observations that were requested.
    """
    return get_nearest_station_observations(point, date_range)


def get_station_within_polygon(polygon: Series, date_range: DateRange) -> list[MetStation]:
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
        json = r.json()
        data = json['data']
        return [MetStation.parse_obj(i) for i in data]


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
