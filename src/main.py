import logging
import os
import pathlib
from datetime import datetime
from fastapi import FastAPI
from starlette.responses import Response, FileResponse, JSONResponse

from TileSetSimulator import TileSetSimulator
from ContinuousDBReader import ContinuousDBReader
from Config import config
from reparcellator.ContinuousDB import ContinuousDB

app = FastAPI()


@app.get("/ping")
async def ping():
    return Response('pong gaga', status_code=200)


@app.get("/config")
def config_data():
    return config.params


@app.get("/tile_set_by_id/{giha_id:int}")
def tile_set(giha_id: int):
    t = TileSetSimulator(db_path=config.params['source'])
    json = t.create_tileset_from_id(giha_id)

    return JSONResponse(json, status_code=200, headers={
        "Access-Control-Allow-Origin": "*"
    })


@app.get("/tile_set/{x:int}/{y:int}/{z:int}")
def tile_set(x: int, y: int, z: int):
    t = TileSetSimulator(db_path=config.params['source'])
    json = t.create_tileset_json_from_tile(x, y, z)

    return JSONResponse(json, status_code=200, headers={
        "Access-Control-Allow-Origin": "*"
    })


@app.get("/tile_path/{x:int}/{y:int}/{z:int}")
def tile_path(x: int, y: int, z: int):
    # db = ContinuousDBReader(config.params['source'])
    # path = db.select_tile_path(x, y, z)
    # @todo: move to global - to connect once
    db = ContinuousDB(config.params['source'])
    path = db.select_path_combine(x, y, z)
    # path = db.select_path_full_dress(x, y, z)

    print('TILE PATH:', path)
    if path is None:
        return Response(status_code=404, headers={
            "Access-Control-Allow-Origin": config.params['Allow-Origin']
        })

    file_path = path
    if not file_path.endswith('.b3dm'):
        file_path += '.b3dm'

    p = pathlib.Path(file_path)
    if p.exists():
        last_modified = p.stat().st_mtime
        headers = {
            "Last-Modified": make_http_time_string(last_modified),
            "Access-Control-Allow-Origin": config.params['Allow-Origin']
        }
        return FileResponse(str(p), filename=p.name, headers=headers)
    logging.error("file not found: ", file_path)
    return Response(status_code=404, headers={
        "Access-Control-Allow-Origin": config.params['Allow-Origin']
    })


@app.get("/tile_path_unique/{x:int}/{y:int}/{z:int}/{id:int}")
def tile_path_unique(x: int, y: int, z: int, id: int):
    # db = ContinuousDBReader(config.params['source'])
    # path = db.select_tile_path(x, y, z)
    # @todo: move to global - to connect once
    db = ContinuousDB(config.params['source'])
    path = db.select_tile_path_unique(x, y, z, id)

    print('TILE PATH:', path)
    if path is None:
        return Response(status_code=404, headers={
            "Access-Control-Allow-Origin": config.params['Allow-Origin']
        })

    file_path = path
    if not file_path.endswith('.b3dm'):
        file_path += '.b3dm'

    p = pathlib.Path(file_path)
    if p.exists():
        last_modified = p.stat().st_mtime
        headers = {
            "Last-Modified": make_http_time_string(last_modified),
            "Access-Control-Allow-Origin": config.params['Allow-Origin']
        }
        return FileResponse(str(p), filename=p.name, headers=headers)
    logging.error("file not found: ", file_path)
    return Response(status_code=404, headers={
        "Access-Control-Allow-Origin": config.params['Allow-Origin']
    })


def make_http_time_string(timestamp):
    """Input timestamp and output HTTP header-type time string"""
    time = datetime.fromtimestamp(timestamp)
    return time.strftime('%a, %d %b %Y %H:%M:%S GMT')

#
#
# @app.get("/3dtiles/{source:int}/{file_path:path}")
# async def read_file(source: int, file_path: str):
#     if 0 <= source < len(config.params['sources']):
#         directory = config.params['sources'][source]
#     else:
#         return Response(status_code=404, headers={
#             "Access-Control-Allow-Origin": config.params['Allow-Origin']
#         })
#
#     p = pathlib.Path(os.path.join(directory, file_path))
#     if p.exists():
#         last_modified = p.stat().st_mtime
#         headers = {
#             "Last-Modified": make_http_time_string(last_modified),
#             "Access-Control-Allow-Origin": config.params['Allow-Origin']
#         }
#         return FileResponse(str(p), headers=headers)
#     else:
#         return Response(status_code=404, headers={
#             "Access-Control-Allow-Origin": config.params['Allow-Origin']
#         })
#
#
# @app.head("/3dtiles/{source:int}/{file_path:path}")
# async def is_exist(source: int, file_path: str):
#     if 0 <= source < len(config.params['sources']):
#         directory = config.params['sources'][source]
#     else:
#         return Response(status_code=404, headers={
#             "Access-Control-Allow-Origin": config.params['sources']['Allow-Origin']
#         })
#
#     p = pathlib.Path(os.path.join(directory, file_path))
#     if p.exists():
#         last_modified = p.stat().st_mtime
#         headers = {
#             "Last-Modified": make_http_time_string(last_modified),
#             "Access-Control-Allow-Origin": config.params['Allow-Origin']
#         }
#         return Response(str(p), headers=headers)
#     else:
#         return Response(status_code=404, headers={
#             "Access-Control-Allow-Origin": config.params['Allow-Origin']
#         })
