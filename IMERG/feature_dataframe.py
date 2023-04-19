"""
feature_dataframe.py: Contains the class 'FeatureDataframe' which inherits the class GeoDataFrame
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

# standard imports:
import warnings
from abc import ABC
from typing import Any

# third-party imports:
import pandas as pd
import geopandas
from geopandas import GeoDataFrame
from pandas import DataFrame

from date_range import DateRange
from MET.frost_api import get_weather_stations

warnings.filterwarnings("ignore")


class FeatureDataframe(ABC, GeoDataFrame):
    """
    FeatureDataframe is an abstract class that can be used as a base for a specific dataset FeatureDataFrame. This way
    we can address, each dataset the same, while they have their own implementations of getting the desired information
    """
    def __init__(self, feature_collection: dict, crs):
        super().__init__(geopandas.GeoDataFrame.from_features(feature_collection,
                                                              crs=crs))


# class NasaDataframe(FeatureDataframe):
#     def __init__(self, feature_collection: dict, date_range: DateRange, tiledb: NasaDataBase):
#         """
#         A NasaDataFrame is an implementation of FeatureDataFrame. It takes in the feature_collection dictionary from
#         the front-end containing all the selected geofeatures. Together with the inputted daterange and tile database,
#         this class is able to get the historical precipitation and historical weather station data for each geofeature.
#
#         :param feature_collection: A dictionary containing all the geofeature the user wants information about.
#         """
#         super().__init__(feature_collection, "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs")
#         self.date_range = date_range
#         self._tiledb = tiledb
#
#     def get_dataframe_with_geometries(self) -> GeoDataFrame:
#         """
#         Makes a copy of the GeoDataframe from the front-end and adds the sub_geom column. This column holds the bounds
#         of each geofeature
#         :return: GeoDataframe
#         """
#         df_with_geometries = pd.concat([self, self.bounds], axis=1)
#         df_with_geometries['sub_geom'] = df_with_geometries.geometry
#         return df_with_geometries
#
#     @staticmethod
#     def _get_percentage_of_coverage(dataframe: GeoDataFrame) -> list[float]:
#         """
#         Using the sub_geom and geometry columns, calculate how much of the geofeature's area is within each cell.
#         This will later be used to factor in the weight of each cell. If the geofeature is a POINT, mark it as having 0%
#         this was it won;t have its weight calculated.
#
#         :param dataframe: GeoDataFrame containing all the geofeatures.
#         :return: A list containing all the percentages of each cell that intersects with a geofeature
#         """
#         return [1 if i.sub_geom.area == 0 else (i["geometry"].intersection(i.sub_geom).area / i.sub_geom.area) for
#                 idx, i in
#                 dataframe.iterrows()]
#
#     @staticmethod
#     def _apply_percentage_to_results(polygons: GeoDataFrame) -> list[list[Any]]:
#         """
#         This method applies the percentage calculated in the "_get_percentage_of_coverage" method and applies them to
#         historical precipitation read from the tile database.
#
#         :param polygons: A GeoDataFrame containing all the polygons requested by the user.
#         :return: A list containing the weighted historical precipitation entries.
#         """
#         applied = [list(np.sum([np.multiply(row['percent_coverage'][index1], value) for index1, value in
#                                 enumerate(row['results'])], axis=0)) for index, row in polygons.iterrows()]
#         return applied
#
#     @staticmethod
#     def half_hour_to_hour(full_dataframe: GeoDataFrame) -> list[list[ndarray]]:
#         """
#         This function is used to convert the half-hourly data from the tile database to hourly precipitation data.
#         This is done because he weather stations and the forecasted precipitation both work with hourly precipitation
#         data.
#
#         :param full_dataframe: A GeoDataFrame with the weights applied to the tile database entries.
#         :return: A list containing the hourly historical precipitation from the tile database.
#         """
#         return [[np.sum(i) for i in i.reshape(int(len(i) / 2), 2)] for i in full_dataframe['results']]
#
#     def join_grids_with_df(self, df: GeoDataFrame, grids: GeoDataFrame) -> GeoDataFrame:
#         """
#         This function helps speed up the read operation of teh tile database, by only keeping the cells that intersect
#         with the geofeatures and all the cells within bounds.
#
#         :param df: GeoDataFrame containing the boundaries of each geofeature.
#         :param grids: A GeoDataFrame containing the polygons representing the cells it the tile database based on the
#         boundaries of the selected geofeatures.
#         :return: A GeoDataFrame only containing the cells and polygons that intersect with one and other.
#         """
#         joined_df = grids.sjoin(df)
#         joined_df["percent_coverage"] = self._get_percentage_of_coverage(joined_df)
#         return joined_df
#
#     @staticmethod
#     def get_box_grids(df) -> GeoDataFrame:
#         """
#         Creates a GeaDataFrame containing polygons representing the cells of the tile database that are within bounds of
#         geofeatures.
#
#         :param df: GeoDataframe containing aal the geofeatures.
#         :return: A GeoDataframe containing all the cells within the bounds of the geofeatures.
#         """
#         grid_geometries = [
#             read_dataframe(world_grid_file_url, bbox=(row['minx'], row['miny'], row['maxx'], row['maxy'])) for
#             index, row in df.iterrows()]
#
#         return geopandas.GeoDataFrame(pd.concat(grid_geometries)).drop_duplicates('label')
#
#     def get_stations(self) -> dict:
#         """
#         Gathers all the historical precipitation data within the date range collected by weather station near or within
#         the geofeatures.
#
#         :return: dict containing the data collected by nearby weather stations based on the selected geofeatures and
#         date range.
#         """
#         stations = get_weather_stations(self.to_wkt(), self.date_range)
#         return stations
#
#     def get_tdb_results(self, grids: GeoDataFrame) -> GeoDataFrame:
#         """
#         Read the historical precipitation of each cell intersecting with a geofeature
#
#         :param grids: A GeoDataFrame containing all the geofeatured and cells that intersect.
#         :return: A GeoDataFrame containing the historical precipitation of each cell with in the date range.
#         """
#         date_index = self._tiledb.tiledb_indexes(self.date_range)
#         result_list = []
#         total_time = 0
#         for index, row in grids.iterrows():
#             index_list = [int(i) for i in row['label'].split(',')]
#             start_time = time.time()
#             tdb_ar = da.from_tiledb(f"PolyMap/Databases/{self._tiledb}", attribute='precipitationCal')
#             result = tdb_ar[date_index[0]:date_index[1] + 1, index_list[1]:index_list[1] + 1,
#                      index_list[0]:index_list[0] + 1]
#             total_time = total_time + (time.time() - start_time)
#             result_list.append(np.nan_to_num(result.reshape(int(len(result))).compute(), nan=-1))
#             total_time = total_time + (time.time() - start_time)
#         grids['results'] = result_list
#         print('total tdb: ', total_time)
#         return grids
#
#     @staticmethod
#     def get_formatted_stations(station_data: dict) -> dict[Any, dict[str, Any]]:
#         """
#         Reformat the stations data and data entries in a way that the front-end understands.
#
#         :param station_data: All station data gather from the relevant weather stations
#         :return: A rearranged dictionary containing the weather stations data
#         """
#         ids = []
#         formatted_stations = {}
#         for i in station_data['points']:
#             if not i['id']:
#                 print('found point without station close')
#             elif i['id'] not in ids:
#                 ids.append(i['id'])
#                 formatted_stations[i['id']] = {'name': i['name'], 'coords': i['coords'], 'distance': i['distance'],
#                                                'precipitation': i['value_list']}
#         for i in station_data['stations_wd']:
#             if i['id'] not in ids:
#                 ids.append(i['id'])
#                 formatted_stations[i['id']] = {'name': i['name'], 'coords': i['coords'], 'precipitation': i['value_list']}
#         for i in station_data['stations_nd']:
#             if i['id'] not in ids:
#                 ids.append(i['id'])
#                 formatted_stations[i['id']] = {'name': i['name'], 'coords': i['coords']}
#
#         return formatted_stations
#
#     def get_results(self) -> tuple[
#         list[tuple[list[list[list[Any]]], list[Any], Any]], list[tuple[list[Any], Any, Any, Any]], dict[
#             Any, dict[str, Any]], list[tuple[list[list[list[Any]]], Any, list[Any]]], list[int]]:
#         """
#         This function collects all the requested data in the FeatureDataFrame by calling the necessary class methods in
#         the right order. It then formats all this data in dictionaries and list so that it can be sent to the front-end
#         javascript to be displayed.
#
#         :return: A tuple containing all the polygons drawn by the user and their results, all point drawn by the user
#         and their results, All weather station data, all intersecting cells and their unweighted results, and a list
#         containing all the hours within the selected date range in unix representation.
#         """
#         df_with_geometries = self.get_dataframe_with_geometries()
#         box_grid = self.get_box_grids(df_with_geometries)
#         stations_data = self.get_stations()
#         data = pd.DataFrame(
#             {'timestamp': self.date_range.unix_list,
#              'precipitation': stations_data['points'][0]['value_list'],
#              })
#         # data.to_csv("C:/Users/tim/PycharmProjects/TileDB project/TimCollections/PolyMap/Precipitation210921-210927.csv")
#         df_with_joined_grids = self.join_grids_with_df(
#             df_with_geometries.drop(['minx', 'miny', 'maxx', 'maxy'], axis=1), box_grid)
#         df_with_tdb_results = self.get_tdb_results(df_with_joined_grids)
#         df_with_tdb_results['results'] = self.half_hour_to_hour(df_with_tdb_results)
#         df_with_points = df_with_tdb_results[df_with_tdb_results["sub_geom"].geom_type == 'Point'].reset_index()
#         df_with_polygons = df_with_tdb_results[df_with_tdb_results["sub_geom"].geom_type == 'Polygon'].groupby(
#             "index_right").agg(list)
#         df_with_polygons['results'] = self._apply_percentage_to_results(df_with_polygons)
#         points = [
#             ([row['sub_geom'].x, row['sub_geom'].y], row['label'].split(","), stations_data['points'][index]['id'], index) for
#             index, row in df_with_points.iterrows()]
#         polygons = [([[list(i) for i in row['sub_geom'][0].exterior.coords]], [i.split(",") for i in row['label']], row['results']) for
#                     index, row in df_with_polygons.iterrows()]
#         stations = self.get_formatted_stations(stations_data)
#         grids = [([[list(i) for i in row['geometry'].exterior.coords]], row['label'].split(","), list(row['results']))
#                  for index, row in df_with_tdb_results.iterrows()]
#         return polygons, points, stations, grids, self.date_range.unix_list


class StationDataframe(FeatureDataframe):
    def __init__(self, feature_collection: dict, date_range: DateRange, crs):
        """
        A NasaDataFrame is an implementation of FeatureDataFrame. It takes in the feature_collection dictionary from
        the front-end containing all the selected geofeatures. Together with the inputted daterange and tile database,
        this class is able to get the historical precipitation and historical weather station data for each geofeature.

        :param feature_collection: A dictionary containing all the geofeature the user wants information about.
        """
        super().__init__(feature_collection, crs)
        self.date_range = date_range

    def get_stations(self) -> dict:
        """
        Gathers all the historical precipitation data within the date range collected by weather station near or within
        the geofeatures.

        :return: dict containing the data collected by nearby weather stations based on the selected geofeatures and
        date range.
        """
        stations = get_weather_stations(self.to_wkt(), self.date_range)
        return stations


    @staticmethod
    def get_formatted_stations(station_data: dict) -> dict[Any, dict[str, Any]]:
        """
        Reformat the stations data and data entries in a way that the front-end understands.

        :param station_data: All station data gather from the relevant weather stations
        :return: A rearranged dictionary containing the weather stations data
        """
        ids = []
        formatted_stations = {}
        for i in station_data['points']:
            if not i['id']:
                print('found point without station close')
            elif i['id'] not in ids:
                ids.append(i['id'])
                formatted_stations[i['id']] = {'name': i['name'], 'coords': i['coords'], 'distance': i['distance'],
                                               'precipitation': i['value_list']}
        for i in station_data['stations_wd']:
            if i['id'] not in ids:
                ids.append(i['id'])
                formatted_stations[i['id']] = {'name': i['name'], 'coords': i['coords'], 'precipitation': i['value_list']}
        for i in station_data['stations_nd']:
            if i['id'] not in ids:
                ids.append(i['id'])
                formatted_stations[i['id']] = {'name': i['name'], 'coords': i['coords']}

        return formatted_stations

    def get_results(self) -> DataFrame:
        """
        This function collects all the requested data in the FeatureDataFrame by calling the necessary class methods in
        the right order. It then formats all this data in dictionaries and list so that it can be sent to the front-end
        javascript to be displayed.

        :return: A tuple containing all the polygons drawn by the user and their results, all point drawn by the user
        and their results, All weather station data, all intersecting cells and their unweighted results, and a list
        containing all the hours within the selected date range in unix representation.
        """
        stations_data = self.get_stations()
        data = pd.DataFrame(
            {'timestamp': self.date_range.unix_list,
             'precipitation': stations_data['points'][0]['value_list'],
             })
        return data
