"""
Pipelife.py: Contains all the functions and classes that interface with the PipeLife API
"""

__author__ = "Tim Rietdijk"
__email__ = "tim.is@live.nl"
__status__ = "Development"

# local imports:
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Any, Dict

# third-party imports:
import pandas as pd
import pytz
import requests
from termcolor import colored

from Schemas.schemas_pipelife import CulvertResults
from exceptions import InvalidPipeLifeCredentials
from date_range import DateRange


class Culvert(ABC):
    """Culvert is an abstract class that can be inherited by any specific culvert, like a PipeLife culvert"""

    def __init__(self, access_token: str, location_id: str, location: list[float]) -> None:
        self._access_token = access_token
        self._location_id = location_id
        self.location = location

    @abstractmethod
    def get_hourly_data(self, date_range: DateRange):
        pass

    @abstractmethod
    def get_images_list(self, date_range: DateRange):
        pass

    def __eq__(self, other):
        if isinstance(other, Culvert):
            return self._location_id == other._location_id and self.location == other.location
        return False


class PipeLifeCulvert(Culvert):
    def __init__(self, access_token: str, location_id: str, location: list[float]) -> None:
        """
        Initiation method for the PipeLifeCulvert class

        :param access_token: String representing the access token for the PipeLife API.
        :param location_id: Int representing the id of the location where you want water level values of.
        :param location: String representing the name of the location owned by PipeLife that processes the culvert
        information.
        :param date_range: The date range of which the user wants the historic water level for specific culverts.
        """
        super().__init__(access_token, location_id, location)
        self._response = self._get_response
        self._water_level_ids = self._get_water_level_ids

    @property
    def _get_response(self) -> dict[any]:
        """
        Store the response tag data, so we don't have to call the PipeLife API constantly. Historic data won't change

        :return: A dictionary containing the tag data for this location
        """
        api_endpoint = f'https://www.telecontrolnet.nl/api/v1/locations/{self._location_id}/tags'
        api_request = requests.get(api_endpoint, {'access_token': self._access_token})
        response = api_request.json()
        return response

    @property
    def _get_water_level_ids(self) -> list[str]:
        """
        Requests the id that corresponds with the water level tag for the inputted location.

        :return: String representing the id for the water level tag.
        """
        water_level_index = \
            [index for index, i in enumerate(self._response['tags']) if i['tag']['description'] == 'Water Level']
        tag_ids = [self._response['tags'][i]['tag']['id'] for i in water_level_index]
        print(tag_ids)
        return tag_ids

    def get_hourly_data(self, date_range: DateRange) -> list[CulvertResults]:
        """
        Request all logged values and timestamps available for this location within the inputted dates.

        :param date_range:
        :param start_time: Integer representing the start date in Unix time.
        :param end_time: Integer representing an end date in Unix time.
        :return: Dataframe containing timestamps and values gathered between the given dates.
        """
        culvert_result_list = []
        for id in self._water_level_ids:
            api_endpoint = f'https://www.telecontrolnet.nl/api/v1/trend/{id}'
            values_request = requests.get(api_endpoint,
                                          {'access_token': self._access_token, "s": date_range.min_date_unix,
                                           'e': date_range.max_date_unix})
            response = values_request.json()
            df = pd.json_normalize(response)
            df['has_image'] = ['null'] * len(df)
            df.drop(columns=['flag'])
            culvert_result_list.append(
                CulvertResults(id=self._location_id, timestamp=list(df['logtime@uts']), values=list(df['logvalue']),
                               images=self.get_images_list(date_range)))
        return culvert_result_list

    def get_tags_from_id(self) -> list[object]:
        """
        Request all logged values and timestamps available for this location within the inputted dates.

        :param start_time: Integer representing the start date in Unix time.
        :param end_time: Integer representing an end date in Unix time.
        :return: Dataframe containing timestamps and values gathered between the given dates.
        """
        api_endpoint = f'https://www.telecontrolnet.nl/api/v1/locations/{self._location_id}/tags'
        values_request = requests.get(api_endpoint, {'access_token': self._access_token})
        response = values_request.json()
        return response

    def get_images_list(self, date_range: DateRange) -> list[dict[str, str | int]] | None:
        """
        Post requests all the timestamps where the Pipelife API has made images of the culvert.

        :param date_range:
        :param start_time: Integer representing the start date in Unix time.
        :param end_time: Integer representing a end date in Unix time.
        :return: A list containing the timestamps where an image was taken.
        """
        try:
            tag_index = \
                [index for index, i in enumerate(self._response['tags']) if i['tag']['description'] == 'Image'][0]
        except Exception as e:
            print(colored(f"{self._location_id}:", 'white'), colored(f" no images available ({e})", 'red'))
            return None
        image_id = self._response['tags'][tag_index]['tag']['id']
        api_endpoint = f'https://www.telecontrolnet.nl/api/v1/trend/{image_id}'
        api_request = requests.get(api_endpoint,
                                   {'access_token': self._access_token, "s": date_range.min_date_unix,
                                    'e': date_range.max_date_unix})
        response = api_request.json()
        url = f"https://www.telecontrolnet.nl/api/v1/locations/{self._location_id}/files"
        response_file = requests.get(url, {'access_token': self._access_token})
        if 'error' in response_file.json():
            print(colored(f"{self._location_id}:", 'white'), colored(f" {response_file.json()['error']}", 'yellow'))
        else:
            dir = ''
            for i in response_file.json():
                if i['name'] == 'webcam':
                    dir = i['id']
            return self._get_image_url(dir, response)

    def _get_image_url(self, dir: str, image_list: list[dict]) -> list[dict[str, str | int]]:
        """
        Try to decipher the url to the requested image. This process will be streamlined by PipeLife in the future.
        Currently, we have to construct the url by adding the timestamp of the supposed image to a string. This has
        chance to fail.

        :param dir: String representing the url to the folder of the requested culvert on the PipeLife server.
        :param image_list: A list containing all the images the user requested.
        :return: A list containing the urls to the requested images.
        """
        image_urls = []
        for i in image_list:
            stamp = datetime.fromtimestamp(int(i['logtime@uts']))
            datelist = list(str(stamp.astimezone(pytz.utc)))
            dir = dir + f'-2f3{datelist[0]}-3{datelist[1]}3{datelist[2]}-3{datelist[3]}2f3{datelist[5]}3{datelist[6]}2f62-6c6f-625f-3' \
                        f'{datelist[0]}3{datelist[1]}-3{datelist[2]}3{datelist[3]}-2d3{datelist[5]}-3{datelist[6]}' \
                        f'2d-3{datelist[8]}3{datelist[9]}-2d3{datelist[11]}-3{datelist[12]}3{datelist[14]}-3{datelist[15]}' \
                        f'3{datelist[17]}-3{datelist[18]}2e-6a70-6567'
            image_urls.append({'timestamp':int(i['logtime@uts']), 'url': f'https://www.telecontrolnet.nl/api/v1/locations/32313138-30/files/{dir}?contents=1&access_token={self._access_token}'})
        return image_urls


class PipeLifeUser:
    def __init__(self, client_id: str, client_secret: str, client_username: str, password: str) -> None:
        """
        Initiation method for the PipeLifeUser class.

        :param client_id: Client ID given to you by the PipeLife team
        :param client_secret: Secret key given to you by the PipeLife team
        :param client_username: Username given to you by the PipeLife team
        :param password: Password given to you by the PipeLife team
        """
        self._client_id = client_id
        self._client_secret = client_secret
        self._client_username = client_username
        self._password = password
        self.access_token = self._get_access_token

    @property
    def _get_access_token(self) -> str:
        """
        Requests an access token for the PipeLife Api using the inputted credentials.

        :return: String used to get access to the PipeLife API, Expires after an hour.
        """
        token_endpoint = 'https://www.telecontrolnet.nl/oauth/token'
        parameters = {
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'grant_type': "password",
            'username': self._client_username,
            'password': self._password,
        }

        token_request = requests.post(token_endpoint, parameters)
        if 'error' in token_request.json():
            raise InvalidPipeLifeCredentials
        return token_request.json()['access_token']

    def get_culverts_from_id_list(self, ids: list[str]) -> list[PipeLifeCulvert]:
        """
        requests all the available locations and returns their ids.

        :return: List containing the culvert classes of the requested locations
        """
        api_endpoint = f'https://www.telecontrolnet.nl/api/v1/locations'
        api_request = requests.get(api_endpoint, {'access_token': self.access_token})
        location_id_list = [
            PipeLifeCulvert(self.access_token, i['location']['id'],
                            [float(i['location']['y']), float(i['location']['x'])]) for i in
            api_request.json()['locations'] if i['location']['id'] in ids]
        return location_id_list

    def __str__(self):
        return self._client_username
