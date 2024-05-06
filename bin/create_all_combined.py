from datetime import datetime

import requests

from TileScheme.WmtsTile import WmtsTile
from reparcellator.ContinuousDB import ContinuousDB

db_path = r'C:\temp\best\best_blend_example_clean_v20.db'
cd = ContinuousDB(db_path)


def calculateBoundaries():
    cd.setDate(datetime.now())
    cd.setId('1')
    # loop all the tiles in the database
    #   if tile is not full_dressed calculate boundary and store in the boundary table
    for x in cd.select_all():
        if x[6] < 1:
            print(x)
            tt = x
            tile_data = {'uri': f'http://localhost:8005/tile_path_unique/{tt[2]}/{tt[3]}/{tt[4]}/{tt[1]}',
                         'polygon': []}

            r = requests.post('http://localhost:8014/derive/boundary',
                              json=tile_data)
            print(f'r.status_code = {r.status_code}')
            print(f'r.content = {r.content}')
            boundary = r.content.decode()
            tile = WmtsTile(tt[2], tt[3], tt[4])
            cd.save_boundary(tile, boundary)


def calculateCombined(z: int):
    # find the area of interest based on the database
    # read the priority table
    # loop the tiles in the database according to the area of interest
    #   if blend required to be run it and store the output in the combined table
    r = cd.select_range(z)
    for t in r.tiles():
        cd.select_path_combine(t.x, t.y, t.z)


# calculateBoundaries()
calculateCombined(17)

print('Done.')
