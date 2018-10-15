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

import nexusproto
from nexusproto.serialization import from_shaped_array

import datetime
import time
import logging
from netCDF4 import Dataset, num2date
from pytz import timezone

from sdap.processors import NexusTileProcessor

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))


class BadTimestampExtractionException(Exception):
    pass

def to_seconds_from_epoch(timestamp, pattern):
    try:
        seconds = int(time.mktime(time.strptime(timestamp, pattern)))
        return seconds
    except ValueError:
        logging.error('{} timestamp is not of the format {}'.format(timestamp, pattern))

class ExtractTimestampProcessor(NexusTileProcessor):

    def __init__(self, timestamp_name, timestamp_pattern, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.timestamp_name = timestamp_name
        self.timestamp_pattern = timestamp_pattern

    def process_nexus_tile(self, nexus_tile):
        output_tile = nexusproto.DataTile_pb2.NexusTile()
        output_tile.CopyFrom(nexus_tile)

        file_path = output_tile.summary.granule
        file_path = file_path[len('file:'):] if file_path.startswith('file:') else file_path

        tile_type = nexus_tile.tile.WhichOneof("tile_type")

        with Dataset(file_path) as ds:
            timestamp = getattr(ds,self.timestamp_name)
            seconds = to_seconds_from_epoch(timestamp, self.timestamp_pattern)

            if tile_type == "grid_tile":
                nexus_tile.tile.grid_tile.time = seconds
            else:
                raise BadTimestampExtractionException

        yield nexus_tile
