"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from os import path

import processors


class TestSummarizeTile(unittest.TestCase):
    def test_summarize_swath(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'smap_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        summarizer = processors.TileSummarizingProcessor()

        results = list(summarizer.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile = results[0]

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('swath_tile'))
        self.assertTrue(nexus_tile.HasField('summary'))

        # Check summary
        tile_summary = nexus_tile.summary
        self.assertAlmostEqual(-50.056, tile_summary.bbox.lat_min, places=3)
        self.assertAlmostEqual(-47.949, tile_summary.bbox.lat_max, places=3)
        self.assertAlmostEqual(22.376, tile_summary.bbox.lon_min, places=3)
        self.assertAlmostEqual(36.013, tile_summary.bbox.lon_max, places=3)

        self.assertAlmostEqual(33.067, tile_summary.stats.min, places=3)
        self.assertEqual(40, tile_summary.stats.max)
        self.assertAlmostEqual(36.6348, tile_summary.stats.mean, places=3)
        self.assertEqual(43, tile_summary.stats.count)

        self.assertEqual(1427820162, tile_summary.stats.min_time)
        self.assertEqual(1427820162, tile_summary.stats.max_time)

    def test_summarize_grid(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'avhrr_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        summarizer = processors.TileSummarizingProcessor()

        results = list(summarizer.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile = results[0]

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('grid_tile'))
        self.assertTrue(nexus_tile.HasField('summary'))

        # Check summary
        tile_summary = nexus_tile.summary
        self.assertAlmostEqual(-39.875, tile_summary.bbox.lat_min, places=3)
        self.assertAlmostEqual(-37.625, tile_summary.bbox.lat_max, places=3)
        self.assertAlmostEqual(-129.875, tile_summary.bbox.lon_min, places=3)
        self.assertAlmostEqual(-127.625, tile_summary.bbox.lon_max, places=3)

        self.assertAlmostEqual(288.5099, tile_summary.stats.min, places=3)
        self.assertAlmostEqual(290.4, tile_summary.stats.max, places=3)
        self.assertAlmostEqual(289.4443, tile_summary.stats.mean, places=3)
        self.assertEqual(100, tile_summary.stats.count)

        self.assertEqual(1462838400, tile_summary.stats.min_time)
        self.assertEqual(1462838400, tile_summary.stats.max_time)


if __name__ == '__main__':
    unittest.main()
