"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from os import path

from nexusproto import NexusContent_pb2 as nexusproto

from processors.processorchain import ProcessorChain


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
