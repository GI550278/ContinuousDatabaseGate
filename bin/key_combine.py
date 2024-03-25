import sqlite3
from datetime import datetime

from TileScheme.WmtsTile import WmtsTile
from reparcellator.ContinuousDB import ContinuousDB

db_path = r'C:\temp\best\best_blend_example_clean_v2.db'

cd = ContinuousDB(db_path)
r = cd.select_path_combine(156182, 42564, 17)
print(r)

cd.close()
print("DONE")
