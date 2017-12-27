"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import datetime

from pytz import timezone

from processors import NexusTileProcessor

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))


class NormalizeTimeBeginningOfMonth(NexusTileProcessor):
    def process_nexus_tile(self, nexus_tile):
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        time = the_tile_data.time

        timeObj = datetime.datetime.utcfromtimestamp(time)

        timeObj = timeObj.replace(day=1)

        timeObj = timezone('UTC').localize(timeObj)

        the_tile_data.time = int((timeObj - EPOCH).total_seconds())

        yield nexus_tile
