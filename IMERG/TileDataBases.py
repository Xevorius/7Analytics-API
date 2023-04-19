"""
datetime.py: Contains all the functionality to do with dates and times for the 7Analytics TileDB test Web application.
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

# standard imports:
import math
import os
import shutil
import time
from abc import abstractmethod, ABC

# third-party imports:
import dask
from dask import array as da
import h5py
import numpy as np
import tiledb

# local imports
# from PolyMap.models import TileDatabase
from date_range import DateRange


# class TileDataBase(ABC):
#     """
#     TileDataBase is an abstract class that can be used as a base for a specific dataset TileDataBase. This way
#     we can address, each database containing a separate dataset the same, while they have their own implementations of
#     getting the desired information.
#     """
#     def __init__(self, database: TileDatabase):
#         self._date_range = DateRange(database.date_range)
#         self._tiledb_config_name = database.tiledb_config_name
#         self._schedular = database.get_schedular_display()
#         self._engine = database.engine
#         self._consolidate = database.consolidate
#         self._drive = database.drive
#         self._dataset = database.dataset
#         self._chunk_x = database.x_chunk_size
#         self._chunk_y = database.y_chunk_size
#         self._chunk_z = database.z_chunk_size
#
#     def __str__(self):
#         return self._tiledb_config_name
#
#     @abstractmethod
#     def tiledb_indexes(self, daterange: DateRange):
#         pass
#
#     @abstractmethod
#     def _daterange_is_valid(self, daterange: DateRange):
#         pass
#
#     @abstractmethod
#     def _create_empty_db(self):
#         pass
#
#     @abstractmethod
#     def get_test_results(self, number_of_runs: int, number_of_reads: int, steps: int) -> float:
#         pass
#
#     @abstractmethod
#     def create_tiledb(self):
#         pass
#
#     @abstractmethod
#     def _prep_database_location(self) -> float:
#         pass


# class NasaDataBase(TileDataBase):
#     """
#     NasaDataBase is a class representation of a tile database containing IMERG data.
#     """
#     @property
#     def _path(self):
#         """
#         returns the url to the database
#         :return: string representing the url to the database
#         """
#         return f"PolyMap/Databases/{self._tiledb_config_name}"
#
#     def tiledb_indexes(self, date_range: DateRange) -> tuple[int, int]:
#         """
#         This function returns the tile database temporal axis index that corresponds with the min and max timestamp of
#         the selected daterange.
#
#         :param date_range: The date range of which the user wants the historic precipitation.
#         :return: tuple containing the indexes of the tiledb that corresponds with the min and max date of the date range
#         """
#         if self._daterange_is_valid(date_range):
#             max_interval_list = [t for t in
#                                  np.arange(self._date_range.min_date,
#                                            self._date_range.max_date + np.timedelta64(30, 'm'),
#                                            np.timedelta64(30, "m"))]
#             return max_interval_list.index(date_range.min_date), max_interval_list.index(date_range.max_date)
#         else:
#             print("Given date range isn't valid")
#
#     def _daterange_is_valid(self, date_range: DateRange) -> bool:
#         """
#         Checks if the selected date range is completely stored in the selected tile database.
#
#         :param date_range: The date range of which the user wants the historic precipitation.
#         :return: boolean representing whether the date range is valid
#         """
#         return self._date_range.min_date <= date_range.min_date <= date_range.max_date <= self._date_range.max_date # and date_range.is_valid()
#
#     def _create_empty_db(self) -> None:
#         """
#         Creates an empty tile database based on the schema stored in this class
#
#         :return: None
#         """
#         n_blocks_z = math.ceil(len(self._date_range.file_location_list) / (self._chunk_z * 1.0))
#         n_blocks_x = math.ceil(3600 / (self._chunk_x * 1.0))
#         n_blocks_y = math.ceil(1800 / (self._chunk_y * 1.0))
#         dom = tiledb.Domain(
#             tiledb.Dim(name='BANDS', domain=(0, (n_blocks_z * self._chunk_z) - 1),
#                        tile=self._chunk_z),
#             tiledb.Dim(name='X', domain=(0, (n_blocks_y * self._chunk_x) - 1),
#                        tile=self._chunk_x, dtype=np.uint64),
#             tiledb.Dim(name='Y', domain=(0, (n_blocks_x * self._chunk_y) - 1),
#                        tile=self._chunk_y, dtype=np.uint64))
#
#         schema = tiledb.ArraySchema(domain=dom, sparse=False,
#                                     attrs=[tiledb.Attr(name='precipitationCal',
#                                                        dtype=np.float64)])
#
#         tiledb.DenseArray.create(self._path, schema)
#
#     def create_tiledb(self) -> float:
#         """
#         This method creates an empty tile database and populates it with the precipitation data that corresponds with
#         the selected date range from the IMERG dataset.
#
#         :return: a float representing the time it took to create and write to the desired database
#         """
#         self._prep_database_location()
#         self._create_empty_db()
#         if self._engine == 2:
#             return self.og_write()
#         else:
#             return self._write_to_db(self._get_lazy_stacked_array_from_raster_list())
#
#     def get_test_results(self, number_of_runs: int, number_of_reads: int, steps: int) -> tuple[
#         list[any], list[any]]:
#         """
#         Runs write and read tests for the selected databases. Returns the time each test took.
#
#         :return: Integers representing the time the test took.
#         """
#         write_results = []
#         read_results = []
#         for i in range(number_of_runs):
#             write_results.append(self.create_tiledb())
#             read_results.append(self._read_db(number_of_reads, steps))
#
#             if self._consolidate:
#                 start_time = time.time()
#                 self._do_consolidation()
#                 print(time.time() - start_time)
#
#         read_arr = np.array(read_results)
#         read_arr_mean = [float(x[0]) for x in zip(list(np.mean(read_arr, axis=0)))]
#
#         write_results.append(np.mean(write_results))
#         write_results.insert(0, self.__str__())
#
#         return write_results, read_arr_mean
#
#     def og_write(self) -> float:
#         """
#         this method passes the HDF5 files and database location to the original tile database write script written by a
#         developer at 7Analytics. After the original write is finished, this function return the time it took to complete
#         that write operation.
#
#         :return: float representing the time the write operation took.
#         """
#         return write_orgi(self._date_range.file_location_list, self._path)
#
#     def _prep_database_location(self) -> float:
#         """
#         This method makes sure that the file location is empty before creating a new tile database.
#
#         :return: float representing the time this preparation took.
#         """
#         start_time = time.time()
#         if not os.path.exists(self._path):
#             os.mkdir(self._path)
#         else:
#             try:
#                 shutil.rmtree(self._path)
#                 os.mkdir(self._path)
#             except FileExistsError:
#                 print(f"Database ("
#                       f"{self._tiledb_config_name}"
#                       f") failed to get removed")
#
#         return time.time() - start_time
#
#     def _get_lazy_stacked_array_from_raster_list(self) -> np.ndarray:
#         """
#         Use Dask to create a lazy 3-dimensional array from the given list of rasters.
#
#         :param rasters: List of validated rasters
#         :param z_chunk_size: Number of arrays per chuck of the z-axis
#         :return: Lazily stacked array of precipitation data from the rasters.
#         """
#         uri_list = self._date_range.file_location_list
#
#         def read_one_file(block_id, axis=0):
#             path = uri_list[block_id[axis]]
#             image = h5py.File(path)['/Grid/precipitationCal']
#             return np.expand_dims(image[0], axis=axis)
#
#         stack = da.map_blocks(read_one_file, dtype=np.float64,
#                               chunks=((1,)*len(uri_list), *h5py.File(uri_list[0])['/Grid/precipitationCal'][0].shape))
#         no_nan = da.where(stack > -9999, stack, np.nan)
#         rot = np.rot90(no_nan, 1, (1, 2))
#         das = rot.rechunk({0: self._chunk_z})
#         return das
#
#     def _write_to_db(self, rasters: np.ndarray) -> float:
#         """
#         This is the new writing method for storing the historical IMERG data. Dask is configured using the variable of
#         this class.
#
#         :param rasters: lazy numpy array containing the historical precipitation data
#         :return: float representing the time teh write operation took.
#         """
#         start_time = time.time()
#         with dask.config.set(scheduler=self._schedular):
#             with tiledb.DenseArray(self._path, 'w') as arr_output:
#                 da.to_tiledb(rasters, arr_output)
#         return time.time() - start_time
#
#     def _read_db(self, number_of_reads: int, steps: int) -> list[any]:
#         """
#         This method runs all the read tests defined by the user.
#
#         :param number_of_reads: integer representing the amount of times each selected database get read tested.
#         :param steps: integer representing by how many tiles each read test gets increased by.
#         :return: list countaining floats representing how long each read test took.
#         """
#         read_range = 1
#         if not steps:
#             read_range = None
#         result_list = []
#         for i in range(number_of_reads):
#             start_time = time.time()
#             tdb_ar = da.from_tiledb(self._path, attribute='precipitationCal')
#             result = tdb_ar[:read_range, 1500:1501, 1500:1501]
#             result.mean().compute()
#             result_list.append(time.time() - start_time)
#             if steps:
#                 read_range = read_range + steps
#         return result_list
#
#     def _do_consolidation(self) -> None:
#         """
#         This method consolidates selected tile databases to test the impact of consolidation of the read and write
#         performance.
#
#         :return: None
#         """
#         tiledb.consolidate(self._path, config=tiledb.Config(
#             {'sm.consolidation.step_min_frags': 2, 'sm.consolidation.step_max_frags': 2, 'sm.consolidation.steps': 1}))
#         tiledb.vacuum(self._path, config=tiledb.Config({'sm.vacuum.mode': 'fragments'}))
#
#     def __str__(self):
#         return self._tiledb_config_name

