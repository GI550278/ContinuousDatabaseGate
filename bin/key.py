# table gihot
# time id priority
import sqlite3
from datetime import datetime

from TileScheme.WmtsTile import WmtsTile
from reparcellator.ContinuousDB import ContinuousDB

db_path = r'C:\temp\best\best_blend_example_clean_v0.db'


# cd.create_new_db()
# #
# cd.add_tile(**{'time': datetime.now(), 'id': '1', 'x': 1, 'y': 1, 'z': 1, 'path': 'path_to_1_in_1', 'full_dress': 1})
# cd.add_tile(**{'time': datetime.now(), 'id': '2', 'x': 1, 'y': 1, 'z': 1, 'path': 'path_to_1_in_2', 'full_dress': 0.8})
# cd.add_tile(**{'time': datetime.now(), 'id': '3', 'x': 1, 'y': 1, 'z': 1, 'path': 'path_to_1_in_3', 'full_dress': 0.8})
#
#
# cd.add_priority(time= datetime.now(), id=1, priority=2)
# cd.add_priority(time= datetime.now(), id=2, priority=1)
class ContinuousDBReaderExtension:
    def __init__(self):

        # p = cd.get_priority()
        # p.sort(key=lambda x: x[2])
        # ids = list(map(lambda x: x[1], p))
        self.ids = ['1', '2']

        self.priority_key = '_'.join(self.ids)
        print('priority:', self.priority_key)
        self.cd = ContinuousDB(db_path)

    def smart_select_tile(self, x, y, z):

        t = self.cd.select_tile_filter(x, y, z, self.ids)
        tiles_count = len(t)
        if tiles_count == 0:
            return

        if tiles_count == 1:
            return t[0]

        t.sort(key=lambda x: self.ids.index(x[1]))
        if t[0][6] == 1:
            return t[0]

        max_k = tiles_count
        for k in range(tiles_count):
            if t[k][6] == 1:
                max_k = k + 1
                break

        t = t[:max_k]
        tile = WmtsTile(x, y, z)
        self.cd.con.commit()
        combined = self.cd.select_combined(tile, self.priority_key)
        print('existing combined:', combined)
        if combined is None:
            print('perform smooth: ', t)
            path = 'smooth_tile_path'
            self.cd.save_combined(tile, path, self.priority_key)
            return (self.priority_key, tile.x, tile.y, tile.z, path)
        return combined


cd = ContinuousDB(db_path)
r = cd.select_path_full_dress(39045, 10640, 15)
print(r)

cd.close()

# cur.execute("CREATE TABLE priority(time REAL,id TEXT,priority INT)")
# con.commit()
#
# time = datetime()
# time_str = time.strftime("%Y-%m-%d %H:%M:%S.%f")
# id = 1
# priority = 2
#
# cur.execute(f"""
#           INSERT INTO priority VALUES
#               (julianday('{time_str}'), '{id}', {priority})
#           """)
# con.commit()
#
# con.close()
print("DONE")
