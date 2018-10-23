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
import unittest
from os import path
import logging

import sdap.processors
from nexusproto import DataTile_pb2 as nexusproto
from sdap.processors.extracttimestampprocessor import BadTimestampExtractionException


class TestExtractTimestamp(unittest.TestCase):
    def setUp(self):
        self.module = sdap.processors.ExtractTimestampProcessor('time_coverage_start', '%Y-%m-%dT%H:%M:%S.000Z')

    def test_extract_timestamp_from_metadata(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_modis.nc')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        input_tile.tile.grid_tile.CopyFrom(nexusproto.GridTile())
        results = list(self.module.process_nexus_tile(input_tile))
        nexus_tile_after = results[0]

        self.assertEqual(1537428301, nexus_tile_after.tile.grid_tile.time)

    def test_extract_timestamp_swath_exception(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_modis.nc')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        input_tile.tile.swath_tile.CopyFrom(nexusproto.SwathTile())

        with self.assertRaises(BadTimestampExtractionException):
            list(self.module.process_nexus_tile(input_tile))[0].tile.swath_tile.time

    def test_extract_timestamp_timeseries_exception(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_modis.nc')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        input_tile.tile.time_series_tile.CopyFrom(nexusproto.TimeSeriesTile())

        with self.assertRaises(BadTimestampExtractionException):
            list(self.module.process_nexus_tile(input_tile))[0].tile.time_series_tile.time

if __name__ == '__main__':
    unittest.main()