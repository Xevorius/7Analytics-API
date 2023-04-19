import requests
from pandas import Series

from date_range import DateRange


def get_station_observations(ids: list[str], date_range: DateRange) -> list[dict]:
    """
    This function gets the historical precipitation measured by the requested weather stations

    :param ids: List of weather station ids
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: returns a list of dictionaries, where a dictionary contains a historic precipitation observation.
    """
    parameters = {
        'sources': ids,
        'elements': 'sum(precipitation_amount PT1H)',
        'referencetime': date_range,
        'fields': 'geometry, value, referenceTime, sourceId',
    }
    try:
        r = requests.get(frost_observation_endpoint, parameters, auth=(frost_client_id, ''))
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


def get_nearest_stations_to_point(point: Series, date_range: DateRange, number_of_nearest_stations: int) -> list[dict]:
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
    r = requests.get(frost_station_endpoint, parameters, auth=(frost_client_id, ''))
    if r.status_code == 200:
        json = r.json()
        data = json['data']
        return data


def get_nearest_station_observations(point: Series, date_range: DateRange) -> dict[str, any]:
    """
    loops through all the weather stations with a complete observation list and returns the closest station to the
    point.

    :param point: A row of the geodataframe containing the information of one point.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: A dictionary representin the closest weather station with a complete observation list to the point
    """
    possible_stations_of_feature = get_nearest_stations_to_point(point, date_range, 100)
    observations = get_station_observations(','.join(str(station['id']) for station in possible_stations_of_feature),
                                            date_range)
    closest_stations_with_full_result_range = get_stations_with_full_observations(possible_stations_of_feature,
                                                                                  observations, date_range)
    if len(closest_stations_with_full_result_range) > 0:
        nearest_station_to_point = {'id': closest_stations_with_full_result_range[0]['id'],
                                    'name': closest_stations_with_full_result_range[0]['name'],
                                    'coords': closest_stations_with_full_result_range[0]['geometry']['coordinates'],
                                    'coords_origin': point[6:].replace('(', '[').replace(')', ']').replace(
                                        ' ', ', '),
                                    'distance': round(float(closest_stations_with_full_result_range[0]['distance']), 1),
                                    'value_list': [i['observations'][0]['value'] for i in observations if
                                                   i['sourceId'].split(':')[0] ==
                                                   closest_stations_with_full_result_range[0]['id']]}
    else:
        nearest_station_to_point = {'id': [], 'name': [], 'coords': [], 'distance': [], 'value_list': []}

    return nearest_station_to_point


def get_stations_with_full_observations(stations: list, observations: list[dict], date_range: DateRange) -> list[
    dict]:
    """
    Returns only the weather stations that have a complete observation list.

    :param stations: List of all the weather stations that are either close to a selected point or within a selected
                     area.
    :param observations: List of precipitation values of all the station in the stations list.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: List of all the weather stations with a complete observation list from closest to furthest away.
    """
    stations_with_values = list(set([i['sourceId'].split(':')[0] for i in observations]))
    stations_with_full_result_range = [i for i in stations if i['id'] in stations_with_values if
                                       len([x for x in observations if
                                            x['sourceId'].split(':')[0] == i['id']]) ==
                                       len(date_range.unix_list)]
    return stations_with_full_result_range


def get_weather_stations(point: Series, date_range: DateRange) -> dict[str, list[dict[str, any]]]:
    """
    This function gets all the historical precipitation based on the user's inputted geofeatures.

    :param feature_frame: Geodataframe containing all the user's inputted geofeatures.
    :param date_range: The date range of which the user wants the historic precipitation.
    :return: A dictionary containing all the weather station and their observations that were requested.
    """
    return get_nearest_station_observations(point, date_range)


def get_station_within_polygon(polygon: Series, date_range: DateRange) -> list[str]:
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
    r = requests.get(frost_station_endpoint, parameters, auth=(frost_client_id, ''))
    if r.status_code == 404:
        json = r.json()
        print('FROST API (Stations): ' + json['error']['reason'])
    if r.status_code == 200:
        json = r.json()
        data = json['data']
        return data
