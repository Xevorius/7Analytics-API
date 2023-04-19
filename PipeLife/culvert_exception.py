"""
culvert_exception.py: Contains all the exceptions used in the Culvert class
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"


class InvalidPipeLifeCredentials(Exception):
    """
    Exception raised when the provided Pipelife credentials aren't valid
    """

    def __init__(self) -> None:
        self.message = f"Provided Pipelife credentials are not recognized. make sure your credentials are valid"
        super().__init__(self.message)
