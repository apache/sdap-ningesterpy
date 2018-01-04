"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array

from processors import NexusTileProcessor

logger = logging.getLogger('emptytilefilter')


def parse_input(nexus_tile_data):
    return nexusproto.NexusTile.FromString(nexus_tile_data)


class EmptyTileFilter(NexusTileProcessor):
    def process_nexus_tile(self, nexus_tile):
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        data = from_shaped_array(the_tile_data.variable_data)

        # Only supply data if there is actual values in the tile
        if data.size - numpy.count_nonzero(numpy.isnan(data)) > 0:
            yield nexus_tile
        elif nexus_tile.HasField("summary"):
            logger.warning("Discarding data %s from %s because it is empty" % (
                nexus_tile.summary.section_spec, nexus_tile.summary.granule))
