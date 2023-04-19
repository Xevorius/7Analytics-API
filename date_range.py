"""
datetime.py: Contains all the functionality to do with dates and times for the 7Analytics TileDB test Web application.
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

import os
# standard imports:
import pathlib
import time
from datetime import datetime

# third-party imports:
import glob
import numpy as np
import pandas as pd
from numpy import datetime64
from pandas import Timestamp

IMERG_PATH = "G:/nasa_rain_data/hdf5/"


class DateRange:
    def __init__(self, date_range: str):
        """
        A class representing a range of dates.

        :param date_range: string with standard format (YYYY-MM-DD/YYYY-MM-DD)
        """
        self._date_range_str = date_range
        self._dates = self._date_range_str.split('/')

    def __str__(self) -> str:
        """
        String representation for this DateTime class

        :return: String representing this DateTime
        """
        return f"{pd.to_datetime(Timestamp(self.min_date)).date()}/" \
               f"{pd.to_datetime(Timestamp(self.max_date + np.timedelta64(1410, 'm'))).date()}"

    @property
    def min_date_str(self) -> str:
        """
        Returns The first day of the date range.

        :return: String representing the first timestamp within date range.
        """
        return str(self._dates[0])

    @property
    def min_date(self) -> datetime64:
        """
        Returns The first day of the date range.

        :return: Datetime64 representing the first timestamp within date range.
        """
        return datetime64(str(self._dates[0]), 'm')

    @property
    def min_date_unix(self) -> int:
        """
        Returns The first day of the date range.

        :return: Integer representing the first timestamp within date range as Unix time
        """
        return int(time.mktime((self.min_date + np.timedelta64(1, 'h')).astype(datetime).timetuple()))

    @property
    def max_date_str(self) -> str:
        """
        Returns The last day of the date range and adds 23,5 hours. The NASA precipitation dataset holds data for every
        half hour. So the last timestamp we need to address for each date range is last date + 23,5 hours

        :return: string representing the last timestamp within date range
        """
        return str(self._dates[1])

    @property
    def max_date(self) -> datetime64:
        """
        Returns The last day of the date range and adds 23,5 hours. The NASA precipitation dataset holds data for every
        half hour. So the last timestamp we need to address for each date range is last date + 23,5 hours

        :return: Datetime64 representing the last timestamp within date range
        """
        return datetime64(str(self._dates[1]), 'm') + np.timedelta64(1410, 'm')

    @property
    def max_date_unix(self) -> int:
        """
        Returns The last day of the date range and adds 23,5 hours. The NASA precipitation dataset holds data for every
        half hour. So the last timestamp we need to address for each date range is last date + 23,5 hours

        :return: Integer representing the last timestamp within date range as Unix time
        """
        return int(time.mktime((self.max_date + np.timedelta64(1, 'h')).astype(datetime).timetuple()))

    @property
    def unix_list(self) -> list[int]:
        """
        Converts the date range string to a list containing the unix time for every hour within that range.

        :param daterange: String containing 2 dates in 'YYYY-MM-DD' divided by a '/'
        :return: List of unix time integers
        """
        hourly_range = np.arange(self.min_date, self.max_date, np.timedelta64(1, "h"))
        return [int(time.mktime(t.astype(datetime).timetuple())) * 1000 for t in hourly_range]

    @property
    def __datetime_list(self) -> list[datetime]:
        """
        Converts the date range string to a list containing the datetime object for every 30 minutes within that range.

        :param daterange: String containing 2 dates in 'YYYY-MM-DD' divided by a '/'
        :return: List of datetime objects
        """
        hourly_range = np.arange(self.min_date, self.max_date + np.timedelta64(30, 'm'), np.timedelta64(30, "m"))
        return [t.astype(datetime) for t in hourly_range]

    @property
    def imerg_date_range(self) -> tuple[datetime64, datetime64]:
        """
        Gets the fist and last timestamp of IMERG data stored on the 7Analytics server

        :return: Tuple of first and last selectable timestamps
        """
        min_date = datetime64(datetime.strptime(pathlib.Path(glob.glob(IMERG_PATH + '*.HDF5')[0]).stem, '%Y%m%d-S%H%M'),
                              'm')
        max_date = datetime64(
            datetime.strptime(pathlib.Path(glob.glob(IMERG_PATH + '*.HDF5')[-1]).stem, '%Y%m%d-S%H%M'), 'm')
        return min_date, max_date

    def is_valid(self) -> bool:
        """
        Checks if the given range is equal to or within 2000-06-01 00:00 and 2021-09-30 23:00.

        :param daterange: String containing 2 dates in 'YYYY-MM-DD' divided by a '/'
        :return: Boolean representing whether given range is within IMERG data range
        """
        try:
            imerg_min_date, imerg_max_date = self.imerg_date_range
            return imerg_min_date <= self.min_date <= self.max_date <= imerg_max_date
        except Exception as e:
            print(f'DateRange error: {e}')
            return False
