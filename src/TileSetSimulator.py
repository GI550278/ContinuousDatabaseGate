import logging

from shapely import Polygon

from TileScheme.TiledPolygon import TiledPolygon
from TileScheme.WmtsTile import WmtsTile
from meshexchange.SimplePolygon import SimplePolygon
from ContinuousDBReader import ContinuousDBReader


class TileSetSimulator:
    """
    Using 3dtile set index created by find_by_extent.py
    Converts the 3dtile set to WMTS children_tiles in given polygon
    """

    def __init__(self, **kwargs):
        self.db_path = kwargs['db_path'] if 'db_path' in kwargs else 'c_test_db.db'
        self.min_level = kwargs['min_level'] if 'min_level' in kwargs else 15
        self.max_level = kwargs['max_level'] if 'max_level' in kwargs else 18

        self.db = ContinuousDBReader(self.db_path)

    def tile_to_json(self, tile):
        geometricError = tile.extent.getGeometricError()
        z_max = 200
        z_min = -200
        region = tile.extent.toRadArray() + [z_min, z_max]
        if tile.z < self.max_level:
            uri = f'/tile_path/{tile.x}/{tile.y}/{tile.z}'
        else:
            uri = f'/tile_set/{tile.x}/{tile.y}/{tile.z}'

        children = []

        if tile.z < self.max_level:
            tiledPolygon = TiledPolygon(self.simple_model_polygon, tile.z + 1)
            print(f'Sub children_tiles of {tile.getName()}:')
            print('-' * 80)
            for child_tile in tile.children():
                if tiledPolygon.isTileIn(child_tile):
                    if self.db.is_tile_exists(child_tile.x, child_tile.y, child_tile.z):
                        children.append(self.tile_to_json(child_tile))
                    else:
                        print(f"Tile {child_tile.getName()} not exists")
                else:
                    print(f"Tile {child_tile.getName()} not in the extent")
                    # print('polygon:' + str(self.simple_model_polygon.polygon))

        return {
            "boundingVolume": {
                "region": region
            },
            "refine": "REPLACE",
            "geometricError": geometricError,
            "content": {
                "uri": uri,
                "boundingVolume": {
                    "region": region
                }
            },
            "children": children
        }

    def create_root(self, tile):
        rad_union_extent = tile.extent
        geometricError = rad_union_extent.getGeometricError()
        root_json = self.tile_to_json(tile)

        return {
            "asset": {
                "version": "1.0"
            },
            "geometricError": geometricError,
            "refine": "REPLACE",
            "root": root_json
        }

    def create_root_multiple(self, children_tiles):
        z_max = 200
        z_min = -200
        rad_union_extent = children_tiles[0].extent
        for tile in children_tiles:
            if rad_union_extent is None:
                rad_union_extent = tile.extent
            else:
                rad_union_extent = tile.extent.union(rad_union_extent)
        region = rad_union_extent.toRadArray() + [z_min, z_max]

        self.simple_model_polygon = SimplePolygon(Polygon(rad_union_extent.transform('32636').asPolygon()))
        print('MUKU KUKU')
        print('polygon:' + str(self.simple_model_polygon.polygon))

        geometricError = rad_union_extent.getGeometricError()
        genealGeometricError = geometricError

        children = []
        for child_tile in children_tiles:
            if self.db.is_tile_exists(child_tile.x, child_tile.y, child_tile.z):
                children.append(self.tile_to_json(child_tile))
                print(f"Tile {child_tile.getName()} added")
            else:
                print(f"Tile {child_tile.getName()} not exists")

        return {
            "asset": {
                "version": "1.0"
            },
            "geometricError": genealGeometricError,
            "refine": "REPLACE",
            "root": {
                "boundingVolume": {
                    "region": region
                },
                "refine": "REPLACE",
                "geometricError": geometricError,
                "children": children
            }
        }

    def create_tileset_json_from_tile(self, x, y, z):
        self.min_level = z
        self.max_level = z + 3
        tile = WmtsTile(x, y, z)

        self.model_polygon = tile.polygon
        self.simple_model_polygon = SimplePolygon(self.model_polygon, '32636')
        self.extent = self.simple_model_polygon.getExtent()
        return self.create_root(tile)
        # return self.create_tileset_json_from_polygon(tile.polygon)

    def create_tileset_json_from_polygon(self, polygon):
        self.model_polygon = polygon
        self.simple_model_polygon = SimplePolygon(self.model_polygon, '32636')
        self.extent = self.simple_model_polygon.getExtent()

        tiledPolygon = TiledPolygon(self.simple_model_polygon, self.min_level)
        tiles = list(tiledPolygon.tiles())

        if tiles is None or len(tiles) == 0:
            logging.error("No start tile")
            return {}

        tileset = self.create_root_multiple(tiles)
        return tileset

    def create_tileset_from_id(self, id):
        try:
            res = self.db.cur.execute(f"SELECT min(z) FROM continuous where id={id}")
            min_z = res.fetchone()[0]
            res = self.db.cur.execute(f"SELECT * FROM continuous where id={id} and z={min_z}")
            tiles = []
            for tile_row in res.fetchall():
                tile = WmtsTile(tile_row[2], tile_row[3], tile_row[4])
                tiles.append(tile)

            return self.create_root_multiple(tiles)
        except Exception as e:
            logging.error("Error creating tileset:" + str(e))
            return {}
