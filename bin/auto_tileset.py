# table gihot
# time id priority
import sqlite3
from datetime import datetime

from TileScheme.WmtsTile import WmtsTile
from reparcellator.ContinuousDB import ContinuousDB

db_path = r'C:\temp\best\best_blend_example_clean_v0.db'
cd = ContinuousDB(db_path)

# cd.select_tile()
id = 1
res = cd.cur.execute(f"SELECT min(z) FROM continuous where id={id}")
min_z = res.fetchone()[0]
print(min_z)
res = cd.cur.execute(f"SELECT * FROM continuous where id={id} and z={min_z}")
tiles = []
for tile_row in res.fetchall():
    tile = WmtsTile(tile_row[2], tile_row[3], tile_row[4])
    tiles.append(tile)

cd.close()
