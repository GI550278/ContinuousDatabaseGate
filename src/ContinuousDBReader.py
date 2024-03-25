import logging
import sqlite3
from datetime import datetime

class ContinuousDBReader:
    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)
        self.cur = self.con.cursor()

    def close(self):
        self.con.close()


    def is_tile_exists(self, x: int, y: int, z: int):
        # @todo: optimize
        res = self.cur.execute(f"SELECT * FROM continuous WHERE x={x} and y={y} and z={z}")
        return not len(res.fetchall()) == 0


    def select_tile(self, x: int, y: int, z: int):
        res = self.cur.execute(f"SELECT * FROM continuous WHERE x={x} and y={y} and z={z} and id='2'")
        return res.fetchall()

    def select_tile_path(self, x: int, y: int, z: int):
        data = self.select_tile(x, y, z)
        if len(data) == 0:
            return None
        # if len(data) < 5:
        #     return None
        return data[0][5]
