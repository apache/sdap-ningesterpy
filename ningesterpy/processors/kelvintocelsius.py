"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from nexusproto.serialization import from_shaped_array, to_shaped_array

from processors import NexusTileProcessor


class KelvinToCelsius(NexusTileProcessor):
    def process_nexus_tile(self, nexus_tile):
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        var_data = from_shaped_array(the_tile_data.variable_data) - 273.15

        the_tile_data.variable_data.CopyFrom(to_shaped_array(var_data))

        yield nexus_tile
