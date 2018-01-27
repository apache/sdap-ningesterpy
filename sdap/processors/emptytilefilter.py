# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from nexusproto import DataTile_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array

from sdap.processors import NexusTileProcessor

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
