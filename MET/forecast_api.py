"""
forecast_api.py: Contains all the functions to interact with MET's FROST API.
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

# local imports:
from metno_locationforecast import Place, Forecast

# standard imports:
import time

# third-party imports:
from geopandas import GeoDataFrame


def get_forecast(features: GeoDataFrame) -> list[list[list[int | float]]]:
    """
    Uses the MET's location API to forecast the precipitation of a point on the map.

    :param features: Geodataframe containing all the features we want to know the forecast of.
    :return: A list containing the precipitation forecast of the requested geofeatures.
    """
    forecasts = []
    for index, row in features.iterrows():
        if row['geometry'].geom_type == 'Point':
            data = get_forecast_data(row.geometry.y, row.geometry.x)
            forecasts.append(data)
        elif row['geometry'].geom_type == 'Polygon':
            center = row.geometry.centroid
            data = get_forecast_data(center.y, center.x)
            forecasts.append(data)
    return forecasts


def get_forecast_data(y, x) -> list[list[int | float]]:
    """
    Formats the forecast data send by the API.

    :param y: Y coordinate of a geofeature.
    :param x: X coordinate of a geofeature.
    :return: A list containing the formatted forecast series.
    """
    data = []
    temp = Place("culvert", y, x)
    forecast = Forecast(temp, "metno-locationforecast/1.0 https://github.com/Rory-Sullivan/metno-locationforecast")
    forecast.update()
    for i in forecast.data.intervals:
        if "precipitation_amount" in i.variables:
            data.append([int(time.mktime(i.start_time.timetuple()) * 1000), i.variables["precipitation_amount"].value])
    return data
