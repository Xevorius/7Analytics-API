import io
import itertools
import time
from typing import Any, List
import geopandas
import pandas as pd
import requests
import shapely
import tiledb
from fastapi import FastAPI, HTTPException
from geopandas import GeoSeries
from geopandas import GeoDataFrame as gpd
import numpy as np
from pyogrio import read_dataframe
from sqlalchemy import create_engine
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, Response
from dotenv import dotenv_values
from MET.forecast_api import get_forecast
from NVE.nve_api import get_nearest_station_obesrvation
from PipeLife.culvert import PipeLifeUser, CulvertResults, PipeLifeCulvert
from Schemas.schemas_met import MetStation
from date_range import DateRange
from MET.met_api import get_nearest_stations_to_point, get_station_within_polygon, get_processed_station_observations, \
    get_processed_station_observations_poly, get_idf_curve_from_nearest_station, get_idf_from_raster

secrets = dotenv_values('.env')

tags_metadata = [
    {
        "name": "MET.NO",
        "description": "All operations interfacing with **MET.NO** data gathered from the FROST API.",
    },
    {
        "name": "IMERG",
        "description": "All operations interfacing with **IMERG** data.",
    },
    {
        "name": "NVE",
        "description": "All operations interfacing with **NVE** data gathered from the NVE API.",
    },
    {
        "name": "PipeLife",
        "description": "All operations interfacing with **PipeLife** data gathered from the PipeLife API.",
    },
    {
        "name": "7A PostGIS",
        "description": "All operations interfacing with the **7Analytics PostGIS** database",
    },
    {
        "name": "Misc",
        "description": "All operations that don't fall under any of the other categories",
    },
]

app = FastAPI(title="Smart Culvert API", version="0.1.3", openapi_tags=tags_metadata, docs_url="/")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

options = {'vfs.s3.aws_access_key_id': secrets['AMAZON_S3_AWS_ACCESS_KEY_ID'],
           'vfs.s3.aws_secret_access_key': secrets['AMAZON_S3_AWS_SECRET_ACCESS_KEY'],
           'vfs.s3.scheme': 'https',
           'vfs.s3.region': 'eu-west-1'
           }

context = tiledb.Config(options)

pipelife_users = [{'TCN_CLIENT_ID': secrets['PIPELIFE_PIPELIFE_CLIENT_ID'],
                   'TCN_CLIENT_SECRET': secrets['PIPELIFE_PIPELIFE_SECRET'],
                   'TCN_MYUSER': secrets['PIPELIFE_PIPELIFE_USER'],
                   'TCN_MYPASS': secrets['PIPELIFE_PIPELIFE_PASSWORD']},
                  {'TCN_CLIENT_ID': secrets['PIPELIFE_BANENOR_CLIENT_ID'],
                   'TCN_CLIENT_SECRET': secrets['PIPELIFE_BANENOR_SECRET'],
                   'TCN_MYUSER': secrets['PIPELIFE_BANENOR_USER'],
                   'TCN_MYPASS': secrets['PIPELIFE_BANENOR_PASSWORD']},
                  {'TCN_CLIENT_ID': secrets['PIPELIFE_KRISTIANSAND_CLIENT_ID'],
                   'TCN_CLIENT_SECRET': secrets['PIPELIFE_KRISTIANSAND_SECRET'],
                   'TCN_MYUSER': secrets['PIPELIFE_KRISTIANSAND_USER'],
                   'TCN_MYPASS': secrets['PIPELIFE_KRISTIANSAND_PASSWORD']},
                  {'TCN_CLIENT_ID': secrets['PIPELIFE_svv_CLIENT_ID'],
                   'TCN_CLIENT_SECRET': secrets['PIPELIFE_svv_SECRET'],
                   'TCN_MYUSER': secrets['PIPELIFE_svv_USER'],
                   'TCN_MYPASS': secrets['PIPELIFE_svv_PASSWORD']}]

"""
Uncomment the following to line if you intend to use the IMERG interface. Keep in mind that it will take a considerable
amount of time to load these variables.
"""
# tbarray = tiledb.open(f'', mode="r", config=context)
# tbarray_dask = da.from_tiledb(f'',
#                          storage_options=options, attribute='precipitationCal')

"""
Uncomment the following line if you want to monitoring the Dask calculations though a Dask diagnostics panel.
"""


# client = Client("tcp://127.0.0.1:63883")


@app.get('/PostGIS/get_pour_points', tags=['7A PostGIS'])
def get_pour_point_feature_collection() -> str:
    """
    Returns all the pour points from the POSTGIS database in a GeoJSON format.
    """
    sql = f"""SELECT * FROM smartculvert.pour_point"""
    con = create_engine("postgresql://postgres:BergenFlom2020!@dev.7analytics.no:5432/postgres")
    dataframe = geopandas.GeoDataFrame.from_postgis(sql, con)
    gdf = dataframe.to_crs(4326)
    return gdf.to_json()


@app.get("/MET/point/nearest", tags=['MET.NO'])
def get_nearest_weather_station(point_wkt: str, date_range: str, crs=4326, output: str = 'python') -> MetStation | None:
    """
    Returns the MET station id closest to the input point with complete precipitation data.
    """
    date_range = DateRange(date_range)
    point = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    station_df = get_nearest_stations_to_point(point, date_range, 1)
    station_df['geometry_origin'] = point_wkt
    if station_df.empty:
        raise HTTPException(status_code=204,
                            detail=f"No MET station was found that is close to the given point. Try another location!")
    if output == 'python':
        station_df['geometry'] = station_df['geometry'].apply(lambda x: x.wkt)
        return MetStation.parse_obj(station_df.iloc[0])
    elif output == 'geojson':
        return Response(content=gpd(station_df, geometry=station_df['geometry'], crs=crs).to_json(),
                        media_type="application/json")
    elif output == 'file':
        stream = io.StringIO()
        station_df.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={station_df.name}.csv"
        return response
    raise HTTPException(status_code=400, detail=f"Export variable: '{output}' not recognized")


@app.get("/MET/point/precipitation", tags=['MET.NO'])
async def get_nearest_full_weather_station(point_wkt: str, date_range: str, crs=4326,
                                           output: str = 'python') -> MetStation | str:
    """
    Returns a **list** or **.CSV** containing the hourly precipitation measured by the weather station closed to the
    input point with complete precipitation data.
    """
    date_range = DateRange(date_range)
    point = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    station_precipitation = get_processed_station_observations(point, date_range)
    station_precipitation['geometry_origin'] = point_wkt
    if station_precipitation.empty:
        raise HTTPException(status_code=204,
                            detail=f"No MET station was found that is close to the given point. Try another location!")
    if output == 'python':
        station_precipitation['geometry'] = station_precipitation['geometry'].apply(lambda x: x.wkt)
        return MetStation.parse_obj(station_precipitation.iloc[0])
    elif output == 'geojson':
        return Response(content=gpd(station_precipitation[station_precipitation.index == 0],
                                    geometry=station_precipitation['geometry'],
                                    crs=crs).to_json(), media_type="application/json")
    elif output == 'file':
        data = pd.DataFrame(
            {'timestamp': date_range.unix_list, 'precipitation': station_precipitation['precipitation']})
        stream = io.StringIO()
        data.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={station_precipitation.name}.csv"
        return response
    raise HTTPException(status_code=400, detail=f"Export variable: '{output}' not recognized")


@app.get("/MET/polygon/all", tags=['MET.NO'])
def get_all_weather_station(polygon_wkt: str, date_range: str, crs=4326, output: str = 'python') -> list[
                                                                                                        MetStation] | str:
    """
    Get all weather stations within the input polygon.
    """
    date_range = DateRange(date_range)
    polygon = GeoSeries.from_wkt([polygon_wkt], crs=crs).to_crs(4326)
    stations = get_station_within_polygon(polygon, date_range)
    stations['geometry_origin'] = polygon_wkt
    if stations.empty:
        raise HTTPException(status_code=204,
                            detail=f"No MET station was found that is close to the given point. Try another location!")
    if output == 'python':
        stations['geometry'] = stations['geometry'].apply(lambda x: x.wkt)
        return [MetStation.parse_obj(i) for _, i in stations.iterrows()]
    elif output == 'geojson':
        return Response(content=gpd(stations, geometry=stations['geometry'],
                                    crs=crs).to_json(), media_type="application/json")
    elif output == 'file':
        data = pd.DataFrame(
            {'timestamp': date_range.unix_list, 'precipitation': stations['precipitation']})
        stream = io.StringIO()
        data.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={stations.name}.csv"
        return response
    raise HTTPException(status_code=400, detail=f"Export variable: '{output}' not recognized")


# 2020-02-01/2020-02-02
# POLYGON((5 60, 6 60, 6 61, 5 61, 5 60))
@app.get("/MET/polygon/precipitation", tags=['MET.NO'])
def get_all_weather_station_precipitation(polygon_wkt: str, date_range: str, crs=4326, output: str = 'python') -> list[MetStation]:
    """
    Returns a list of the hourly precipitation of all weather stations within the input polygon with full precipitation.
    """
    date_range = DateRange(date_range)
    polygon = GeoSeries.from_wkt([polygon_wkt], crs=crs).to_crs(4326)
    station_precipitation = get_processed_station_observations_poly(polygon, date_range)
    station_precipitation['geometry_origin'] = polygon_wkt
    if station_precipitation.empty:
        raise HTTPException(status_code=204,
                            detail=f"No MET station was found that is close to the given point. Try another location!")
    if output == 'python':
        station_precipitation['geometry'] = station_precipitation['geometry'].apply(lambda x: x.wkt)
        return [MetStation.parse_obj(i) for _, i in station_precipitation.iterrows()]
    elif output == 'geojson':
        return Response(content=gpd(station_precipitation, geometry=station_precipitation['geometry'],
                                    crs=crs).to_json(), media_type="application/json")
    elif output == 'file':
        data = pd.DataFrame(
            {'timestamp': date_range.unix_list, 'precipitation': station_precipitation['precipitation']})
        stream = io.StringIO()
        data.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={station_precipitation.name}.csv"
        return response
    raise HTTPException(status_code=400, detail=f"Export variable: '{output}' not recognized")


@app.get("/MET/forecast", tags=['MET.NO'])
def get_forecast_from_shape(shape_wkt: str, crs=4326) -> list[list[list[int | float]]]:
    """
    Returns a list containing the forecasted precipitation of the input shape.
    """
    pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(shape_wkt)], crs=crs).to_crs(4326)
    forecast = get_forecast(pointer)
    return forecast


@app.get("/IMERG/point/precipitation", tags=['IMERG'])
async def get_imerg_precipitation_from_point(point_wkt: str, date_range: str, file: bool = False, crs: int = 4326,
                                             dask: bool = False):
    """
    Access the full historical IMERG precipitation (2000-06-01/2021-09-31). Keep in mind that this dataset is not fully
    optimized yet. Thus, the more data you request the longer your request will take.
    (1 day = 1 second~ | 1 year = 130 seconds~)

    :param point_wkt: String representation of the wkt point you want to get the precipitation from.
    :param date_range: Define the temporal range of the data you want. Format: YYYY-MM-DD/YYYY-MM-DD.
    :param file: (default = False) If true, the output will be a .csv file instead of a list.
    :param crs: (default = 4326) Define the CRS your wkt point is in.
    :param dask: (default = False) Not implemented yet!
    :return:
    """
    date_range = DateRange(date_range)
    point = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    df_with_geometries = pd.concat([point, point.bounds], axis=1)
    df_with_geometries.rename(columns={df_with_geometries.columns[0]: 'geometry'}, inplace=True)
    df_with_geometries['sub_geom'] = df_with_geometries.geometry
    grid_geometries = read_dataframe('IMERG/Grid.fgb', bbox=(
        df_with_geometries['minx'], df_with_geometries['miny'], df_with_geometries['maxx'], df_with_geometries['maxy']))
    gdf = geopandas.GeoDataFrame(grid_geometries).drop_duplicates('label')
    joined_df = gdf.sjoin(geopandas.GeoDataFrame(
        df_with_geometries.drop(['minx', 'miny', 'maxx', 'maxy'], axis=1),
        geometry=df_with_geometries.geometry))
    joined_df["percent_coverage"] = 1
    max_interval_list = [t for t in
                         np.arange(date_range.min_date,
                                   date_range.max_date + np.timedelta64(30, 'm'),
                                   np.timedelta64(30, "m"))]
    max_interval_list.index(date_range.min_date), max_interval_list.index(date_range.max_date)
    date_index = max_interval_list.index(date_range.min_date), max_interval_list.index(date_range.max_date)
    joined_df = joined_df.iloc[0]
    index_list = [int(i) for i in joined_df['label'].split(',')]
    start_time = time.time()
    if dask:
        print("dask")
        result = tbarray_dask[date_index[0]:date_index[1] + 1, index_list[1]:index_list[1] + 1,
                 index_list[0]:index_list[0] + 1]
        reshaped = np.nan_to_num(result.reshape(int(len(result))).compute(), nan=-1)
    else:
        print("numpy")
        result = np.array(tbarray[date_index[0]:date_index[1] + 1, index_list[1]:index_list[1] + 1,
                          index_list[0]:index_list[0] + 1]['precipitationCal'])
        reshaped = np.nan_to_num(result.reshape(int(result.size)), nan=-1)

    results = [np.sum(i) for i in reshaped.reshape(int(len(reshaped) / 2), 2).round(5)]
    print('Full calculation took: ', time.time() - start_time, ' seconds.')
    if file:
        data = pd.DataFrame(
            {'timestamp': date_range.unix_list,
             'precipitation': results,
             })
        stream = io.StringIO()
        data.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]),
                                     media_type="text/csv"
                                     )
        response.headers["Content-Disposition"] = f"attachment; filename={'IMERG'}.csv"
        return response

    else:
        return results


@app.get("/IMERG/polygon/precipitation", tags=['IMERG'])
def get_imerg_precipitation_from_polygon(polygon_wkt: str, date_range: str, crs=4326) -> list[dict]:
    """
    (_Not implemented yet!_)

    :param crs: /
    :param polygon_wkt: /
    :param date_range: /
    :return: /
    """
    date_range = DateRange(date_range)
    polygon = GeoSeries.from_wkt([polygon_wkt], crs=crs).to_crs(4326)
    return [{'Warning': 'Not implemented yet!'}]


@app.get("/NVE/point/flow", tags=['NVE'])
def get_closest_culvert_data(point_wkt: str, date_range: str, crs=4326) -> Response:
    """
    Returns hourly water level and/or water flow (depending on what is available) of the closest culvert to the input
    point.
    """
    date_range = DateRange(date_range)
    pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(point_wkt)], crs=crs).to_crs(4326)
    df = get_nearest_station_obesrvation(pointer, date_range)
    return Response(content=df.to_json(), media_type="application/json")


@app.get("/NVE/polygon/flow", tags=['NVE'])
def get_all_culvert_data_within_polygon(polygon_wkt: str, date_range: str, crs=4326) -> list[Any]:
    """
     Should return hourly water level and/or water flow (depending on what is available) of the culverts within a given
     polygon. But the NVE API reaches an undocumented error. Even with their own example.
     """
    # date_range = DateRange(date_range)
    # pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(polygon_wkt)], crs=crs).to_crs(4326)
    #
    # url = f'https://hydapi.nve.no/api/v1/Stations?Active=1&Polygon={polygon_wkt}'
    # request_headers = {
    #     "Accept": "application/json",
    #     "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    # }
    # nve_stations = requests.get(url, headers=request_headers)
    # print(nve_stations)
    # parsed_result = nve_stations.json()
    # df = pd.json_normalize(parsed_result['data'])
    # print(df)
    # df['parameters'] = [[x['parameter'] for x in i] for i in df.seriesList]
    # dff = df[df['parameters'].apply(lambda x: 1000 in x or 1001 in x)]
    # dff['seriesList'] = [[x for x in i if x['parameter'] == 1000 or x['parameter'] == 1001] for i in dff.seriesList]
    # dff['dateRange'] = [[[DateRange(f"{y['dataFromTime'][:10]}/{y['dataToTime'][:10]}") for y in x['resolutionList'] if
    #                       y['resTime'] == 60] for x in i] for i in dff.seriesList]
    # dfff = dff[dff['dateRange'].apply(lambda x: bool(list(filter(None, x))))]
    # gdf = geopandas.GeoDataFrame(
    #     dfff[["stationId", "stationName", "latitude", "longitude", "seriesList"]],
    #     geometry=geopandas.points_from_xy(dfff.longitude, dfff.latitude), crs=4326)
    # print(gdf)
    # culvert = pointer.sjoin_nearest(gdf).merge(gdf, left_on="index_right", right_index=True)
    # culvert_id = culvert.iloc[0]['stationId_x']
    # parameters = {
    #     'StationId': culvert_id,
    #     'Parameter': '1000,1001',
    #     'ResolutionTime': 60,
    #     'ReferenceTime': date_range
    # }
    # url = f"https://hydapi.nve.no/api/v1/Observations"
    # request_headers = {
    #     "Accept": "application/json",
    #     "X-API-Key": "JkbAM/hEkk+5Z7mJIlC3fQ==",
    # }
    # nve_stations = requests.get(url, parameters, headers=request_headers)
    # df = pd.json_normalize(nve_stations['data'], max_level=0)
    # print(df)
    return "The NVE API hasn't implemented this feature correctly..."


@app.get("/MET/point/idf_from_raster", tags=['MET.NO'])
def get_raster_idf_from_point(point_wkt: str, crs: int = 4326) -> Response:
    """
    Returns hourly water level and/or water flow (depending on what is available) of the closest culvert to the input
    point.
    """

    pointer = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    idf = get_idf_from_raster(pointer)

    return Response(content=idf, media_type="application/json")


@app.get("/MET/point/idf_from_station", tags=['MET.NO'])
def get_idf_from_point(point_wkt: str, crs: int = 4326) -> Response:
    """
    Returns hourly water level and/or water flow (depending on what is available) of the closest culvert to the input
    point.
    """

    pointer = GeoSeries.from_wkt([point_wkt], crs=crs).to_crs(4326)
    idf = get_idf_curve_from_nearest_station(pointer)

    return Response(content=idf, media_type="application/json")


@app.get("/PipeLife/id/waterlevel", tags=['PipeLife'])
def get_water_level_from_id(pipelife_ids: str, date_range: str, pipelife_user: str | None = None) -> list[list[CulvertResults]] | list[Any]:  # , verify:bool = True
    """
    Returns a list containing hourly water level data of the input culvert.
    """
    date_range = DateRange(date_range)
    if pipelife_user:
        user = [i for i in pipelife_users if i['TCN_MYUSER'] == pipelife_user]
        if not user:
            raise HTTPException(status_code=400, detail=f'Pipelife User: "{pipelife_user}", is not recognized.')
        pipe_life_user = PipeLifeUser(client_id=user[0]['TCN_CLIENT_ID'], client_secret=user[0]['TCN_CLIENT_SECRET'],
                                      client_username=user[0]['TCN_MYUSER'], password=user[0]['TCN_MYPASS'])
        # if not verify:
        #     location_id_list = [
        #         PipeLifeCulvert(pipe_life_user.access_token, int(i),
        #                         [float(i['location']['y']), float(i['location']['x'])]) for i in pipelife_ids]
        selected_culverts = pipe_life_user.get_culverts_from_id_list(pipelife_ids.split(','))
        if not selected_culverts:
            raise HTTPException(status_code=400, detail=f'Pipelife id(s) do not belong to {pipelife_user}')
        all_pipes_data = [culvert.get_hourly_data(date_range) for culvert in selected_culverts]
        if not all_pipes_data:
            raise HTTPException(status_code=400, detail=f'No Data found for date and culvert combination.')
        return all_pipes_data
    else:
        pipelife_users_list = [PipeLifeUser(client_id=user['TCN_CLIENT_ID'], client_secret=user['TCN_CLIENT_SECRET'],
                                            client_username=user['TCN_MYUSER'], password=user['TCN_MYPASS']) for user in
                               pipelife_users]
        selected_culverts = list(itertools.chain(
            *[x.get_culverts_from_id_list(pipelife_ids.split(',')) for x in pipelife_users_list]))
        all_pipes_data = [culvert.get_hourly_data(date_range) for culvert in selected_culverts]
        return all_pipes_data


@app.get("/PipeLife/id/tags", tags=['PipeLife'])
def get_tags_from_id(pipelife_ids: str, pipelife_user: str | None = None) -> list[Any]:
    """
    Returns a list containing hourly water level data of the input culvert.
    """
    if pipelife_user:
        user = [i for i in pipelife_users if i['TCN_MYUSER'] == pipelife_user]
        if not user:
            raise HTTPException(status_code=400, detail=f'Pipelife User: "{pipelife_user}", is not recognized.')
        pipe_life_user = PipeLifeUser(client_id=user[0]['TCN_CLIENT_ID'], client_secret=user[0]['TCN_CLIENT_SECRET'],
                                      client_username=user[0]['TCN_MYUSER'], password=user[0]['TCN_MYPASS'])
        selected_culverts = pipe_life_user.get_culverts_from_id_list(pipelife_ids.split(','))
        if not selected_culverts:
            raise HTTPException(status_code=400, detail=f'Pipelife id(s) do not belong to {pipelife_user}')
        all_pipes_data = [culvert.get_tags_from_id() for culvert in selected_culverts]
        if not all_pipes_data:
            raise HTTPException(status_code=400, detail=f'No Data found for date and culvert combination.')
        return all_pipes_data
    else:
        pipelife_users_list = [PipeLifeUser(client_id=user['TCN_CLIENT_ID'], client_secret=user['TCN_CLIENT_SECRET'],
                                            client_username=user['TCN_MYUSER'], password=user['TCN_MYPASS']) for user in
                               pipelife_users]
        selected_culverts = list(itertools.chain(
            *[x.get_culverts_from_id_list(pipelife_ids.split(',')) for x in pipelife_users_list]))
        all_pipes_data = [culvert.get_tags_from_id() for culvert in selected_culverts]
    return all_pipes_data


@app.get("/Misc/check_all_datasets", tags=['Misc'])
def get_forecast_from_point(point_wkt: str, date_range: str, crs=4326) -> dict[str, str | bool]:
    """
    Returns a list containing the forecasted precipitation of the input point.
    """
    # date_range = DateRange(date_range)
    # pointer = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(point_wkt)], crs=crs).to_crs(4326)

    get_nearest_full_weather_station(point_wkt, date_range, crs=crs)

    return {'NVE': False, 'MET': False, 'IMERG': False, 'Local_Grid': "Under construction"}
