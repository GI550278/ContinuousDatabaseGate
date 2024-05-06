import sqlite3
from datetime import datetime

from TileScheme.WmtsTile import WmtsTile
from reparcellator.ContinuousDB import ContinuousDB

db_path = r'C:\temp\best\best_blend_example_clean_v20.db'

cd = ContinuousDB(db_path)
r = cd.select_path_combine(78091, 21283, 16)
# r = cd.select_path_combine(39046,10640,15)
print(r)

cd.close()
print("DONE")
