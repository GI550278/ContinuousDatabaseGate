import logging
import sqlite3
from datetime import datetime

from TileScheme.WmtsTile import WmtsTile
import requests


class ContinuousDB:
    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)
        self.cur = self.con.cursor()

        self.ids = ['1', '2']
        self.priority_key = '_'.join(self.ids)

        # @todo: take data from input parameters
        out_name = 'best_blend_example_clean_v2'
        self.dst = rf'C:\Users\sguya\Downloads\cesium-starter-app-master\public\blend_example\{out_name}'

    def close(self):
        self.con.close()

    def create_new_db(self):
        self.cur.execute("CREATE TABLE continuous(time REAL, id TEXT, x INT, y INT, z INT, path TEXT, full_dress INT)")
        self.cur.execute("CREATE TABLE priority(time REAL, id TEXT, priority INT)")
        self.cur.execute("CREATE TABLE combined(key TEXT, x INT, y INT, z INT, path TEXT)")
        self.cur.execute("CREATE TABLE boundary(time REAL, id TEXT, x INT, y INT, z INT, extent TEXT)")
        self.con.commit()

    def add_tile(self, **kwargs):
        time = kwargs['time'] if 'time' in kwargs else None
        if time == None:
            logging.error("cannot add tile, time missing")
            return
        if not isinstance(time, datetime):
            logging.error("cannot add tile, wrong time type. datetime expected")
            return
        time_str = time.strftime("%Y-%m-%d %H:%M:%S.%f")

        id = str(kwargs['id']) if 'id' in kwargs else None
        x = int(kwargs['x']) if 'x' in kwargs else None
        y = int(kwargs['y']) if 'y' in kwargs else None
        z = int(kwargs['z']) if 'z' in kwargs else None
        path = kwargs['path'] if 'path' in kwargs else None
        full_dress = kwargs['full_dress'] if 'full_dress' in kwargs else None

        self.cur.execute(f"""
            INSERT INTO continuous VALUES
                (julianday('{time_str}'), '{id}', {x},{y},{z},"{path}",{full_dress})
            """)
        self.con.commit()

    def select_tile_filter(self, x: int, y: int, z: int, ids: list[str]):
        sql = f"SELECT * FROM continuous WHERE x={x} and y={y} and z={z} and id in ({','.join(ids)})"
        print('sql:', sql)
        res = self.cur.execute(sql)
        return res.fetchall()

    def select_tile(self, x: int, y: int, z: int):
        res = self.cur.execute(f"SELECT * FROM continuous WHERE x={x} and y={y} and z={z}")
        return res.fetchall()

    def select_tile_path(self, x: int, y: int, z: int):
        data = self.select_tile(x, y, z)
        if len(data) == 0:
            return None
        if len(data) < 6:
            return None
        return data[0][5]

    def select_tile_path_unique(self, x: int, y: int, z: int, id: int):
        res = self.cur.execute(f"SELECT * FROM continuous WHERE x={x} and y={y} and z={z} and id={id}")
        data = res.fetchone()
        if len(data) == 0:
            return None
        if len(data) < 6:
            return None
        return data[5]

    def setDate(self, d: datetime):
        self.default_date = d.strftime("%Y-%m-%d %H:%M:%S.%f")

    def setId(self, id: str):
        self.default_id = id

    def save_tile(self, tile: WmtsTile, path: str):
        time_str = self.default_date
        id = self.default_id
        x = tile.x
        y = tile.y
        z = tile.z
        full_dress = 1

        self.cur.execute(f"""
                INSERT INTO continuous VALUES
                    (julianday('{time_str}'), '{id}', {x},{y},{z},"{path}",{full_dress})
                """)
        self.con.commit()

    def add_priority(self, **kwargs):
        time = kwargs['time'] if 'time' in kwargs else None
        if time == None:
            logging.error("cannot add tile, time missing")
            return
        if not isinstance(time, datetime):
            logging.error("cannot add tile, wrong time type. datetime expected")
            return
        time_str = time.strftime("%Y-%m-%d %H:%M:%S.%f")

        id = str(kwargs['id']) if 'id' in kwargs else None
        priority = int(kwargs['priority']) if 'priority' in kwargs else None

        self.cur.execute(f"""
            INSERT INTO priority VALUES
                (julianday('{time_str}'), '{id}', {priority})
            """)
        self.con.commit()

    def get_priority(self):
        res = self.cur.execute(f"SELECT * FROM priority")
        return res.fetchall()

    def save_combined(self, tile: WmtsTile, path: str, key: str):
        x = tile.x
        y = tile.y
        z = tile.z

        self.cur.execute(f"""
                        INSERT INTO combined VALUES
                            ("{key}", {x},{y},{z},"{path}")
                        """)
        self.con.commit()

    def select_combined(self, tile, priority_key):
        x = tile.x
        y = tile.y
        z = tile.z

        res = self.cur.execute(f"SELECT * FROM combined WHERE [key]=\"{priority_key}\" and x={x} and y={y} and z={z}")
        return res.fetchone()

    def select_path_combine(self, x, y, z):
        t = self.select_tile_filter(x, y, z, self.ids)
        tiles_count = len(t)
        if tiles_count == 0:
            return None

        if tiles_count == 1:
            print("THE ONLY TILE:")
            return t[0][5]

        t.sort(key=lambda x: self.ids.index(x[1]))
        if t[0][6] == 1:
            return t[0][5]

        # cut tile array until first full dressed tile
        max_k = tiles_count
        for k in range(tiles_count):
            if t[k][6] == 1:
                max_k = k + 1
                break
        t = t[:max_k]

        tile = WmtsTile(x, y, z)

        self.con.commit()
        combined = self.select_combined(tile, self.priority_key)
        print('existing combined:', combined)
        # combined = None
        if combined is None:
            # print('perform smooth: ', t)
            tiles = []
            for tt in t:
                tile_data = {'uri': f'http://localhost:8005/tile_path_unique/{tt[2]}/{tt[3]}/{tt[4]}/{tt[1]}',
                             'polygon': []}

                tiles.append(tile_data)
# 'polygon': list(tile.polygon.exterior.coords),
            payload = {'tiles': tiles,  'format': 'b3dm',
                       'buffer_length': 0}



            r = requests.post('http://localhost:8004/editor/blend',
                              json=payload)
            print(f'r.status_code = {r.status_code}')

            if int(r.status_code) < 300:
                path = self.dst + tile.getFullPath(self.priority_key)
                with open(path, 'wb') as output:
                    output.write(r.content)
                self.save_combined(tile, path, self.priority_key)
                return path
            else:
                return None
        return combined[4]

    def select_path_full_dress(self, x, y, z):
        t = self.select_tile_filter(x, y, z, self.ids)
        tiles_count = len(t)
        if tiles_count == 0:
            return None

        if tiles_count == 1:
            print("THE ONLY TILE:")
            return t[0][5]

        t.sort(key=lambda x: self.ids.index(x[1]))
        for t_o in t:
            if t_o[6] == 1:
                print("FIRST TO HAVE FULL DRESS:")
                return t_o[5]

        print("NO FULL DRESS IN SET, GETTING FIRST:")
        return t[0][5]

    def is_tile_exists(self, x: int, y: int, z: int):
        # @todo: optimize
        res = self.cur.execute(f"SELECT * FROM continuous WHERE x={x} and y={y} and z={z}")
        return not len(res.fetchall()) == 0
