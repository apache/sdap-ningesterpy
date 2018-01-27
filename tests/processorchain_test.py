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

from nexusproto import DataTile_pb2 as nexusproto

from ningesterpy.processors.processorchain import ProcessorChain


class TestRunChainMethod(unittest.TestCase):
    def test_run_chain_read_filter_all(self):
        processor_list = [
            {'name': 'GridReadingProcessor',
             'config': {'latitude': 'lat',
                        'longitude': 'lon',
                        'time': 'time',
                        'variable_to_read': 'analysed_sst'}},
            {'name': 'EmptyTileFilter'}
        ]
        processorchain = ProcessorChain(processor_list)

        test_file = path.join(path.dirname(__file__), 'datafiles', 'empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        gen = processorchain.process(input_tile)
        for message in gen:
            self.fail("Should not produce any messages. Message: %s" % message)

    def test_run_chain_read_filter_none(self):
        processor_list = [
            {'name': 'GridReadingProcessor',
             'config': {'latitude': 'lat',
                        'longitude': 'lon',
                        'time': 'time',
                        'variable_to_read': 'analysed_sst'}},
            {'name': 'EmptyTileFilter'}
        ]
        processorchain = ProcessorChain(processor_list)

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        results = list(processorchain.process(input_tile))

        self.assertEqual(1, len(results))

    def test_run_chain_read_filter_kelvin_summarize(self):
        processor_list = [
            {'name': 'GridReadingProcessor',
             'config': {'latitude': 'lat',
                        'longitude': 'lon',
                        'time': 'time',
                        'variable_to_read': 'analysed_sst'}},
            {'name': 'EmptyTileFilter'},
            {'name': 'KelvinToCelsius'},
            {'name': 'TileSummarizingProcessor'}
        ]
        processorchain = ProcessorChain(processor_list)

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        results = list(processorchain.process(input_tile))

        self.assertEqual(1, len(results))

    def test_run_chain_partial_empty(self):
        processor_list = [
            {'name': 'GridReadingProcessor',
             'config': {'latitude': 'lat',
                        'longitude': 'lon',
                        'time': 'time',
                        'variable_to_read': 'analysed_sst'}},
            {'name': 'EmptyTileFilter'},
            {'name': 'KelvinToCelsius'},
            {'name': 'TileSummarizingProcessor'}
        ]
        processorchain = ProcessorChain(processor_list)

        test_file = path.join(path.dirname(__file__), 'datafiles', 'partial_empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:489:499,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        results = list(processorchain.process(input_tile))

        self.assertEqual(1, len(results))
        tile = results[0]
        self.assertTrue(tile.summary.HasField('bbox'), "bbox is missing")

        input_tile = nexusproto.NexusTile()
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        input_tile.summary.CopyFrom(tile_summary)

        results = list(processorchain.process(input_tile))

        self.assertEqual(0, len(results))


if __name__ == '__main__':
    unittest.main()
