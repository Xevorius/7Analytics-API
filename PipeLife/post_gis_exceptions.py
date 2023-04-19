"""
post_gis_exceptions.py: Contains all the exceptions used in the post_gis script
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

from sqlalchemy.exc import ProgrammingError


class InvalidLocationIdList(Exception):
    """
    Exception raised when the location id list contains ids not present in the PostGIS database
    """

    def __init__(self, location_id_list: list[str]) -> None:
        """
        Takes in a list of PipeLife ids which will than be checked. All the ids that aren't valid will than be displayed
        to the user

        :param location_id_list: List containing strings representing the PipeLife ids
        """
        wrong_ids = []
        for i in location_id_list:
            try:
                from pipelife.scripts.post_gis import get_geo_dataframe
                _ = get_geo_dataframe([i], validation=True)
            except ProgrammingError:
                wrong_ids.append(i)

        self.message = f"The provided location id list contains these invalid ids: {wrong_ids}"
        super().__init__(self.message)
