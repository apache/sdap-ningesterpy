"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from nexusproto.serialization import from_shaped_array, to_shaped_array

from processors import NexusTileProcessor


class Subtract180Longitude(NexusTileProcessor):
    def process_nexus_tile(self, nexus_tile):
        """
        This method will transform longitude values in degrees_east from 0 TO 360 to -180 to 180

        :param self:
        :param nexus_tile: The nexus_tile
        :return: Tile data with altered longitude values
        """

        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        longitudes = from_shaped_array(the_tile_data.longitude)

        # Only subtract 360 if the longitude is greater than 180
        longitudes[longitudes > 180] -= 360

        the_tile_data.longitude.CopyFrom(to_shaped_array(longitudes))

        yield nexus_tile

