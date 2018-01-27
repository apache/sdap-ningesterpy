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

import ningesterpy.processors


class TestSummarizeTile(unittest.TestCase):
    def test_summarize_swath(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'smap_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        summarizer = ningesterpy.processors.TileSummarizingProcessor()

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

        summarizer = ningesterpy.processors.TileSummarizingProcessor()

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
