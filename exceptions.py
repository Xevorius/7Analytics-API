"""
culvert_exception.py: Contains all the exceptions used in the Culvert class
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

from sqlalchemy.exc import ProgrammingError


class InvalidPipeLifeCredentials(Exception):
    """
    Exception raised when the provided Pipelife credentials aren't valid
    """

    def __init__(self) -> None:
        self.message = f"Provided Pipelife credentials are not recognized. make sure your credentials are valid"
        super().__init__(self.message)


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

                _ = get_geo_dataframe([i], validation=True)
            except ProgrammingError:
                wrong_ids.append(i)

        self.message = f"The provided location id list contains these invalid ids: {wrong_ids}"
        super().__init__(self.message)