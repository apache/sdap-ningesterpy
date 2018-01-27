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
import numpy as np
from nexusproto.serialization import from_shaped_array

import sdap.processors


class TestAscatbUData(unittest.TestCase):

    def test_subtraction_longitudes_less_than_180(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        nexus_tile_before = nexusproto.NexusTile.FromString(nexustile_str)
        longitudes_before = from_shaped_array(nexus_tile_before.tile.swath_tile.longitude)

        subtract = sdap.processors.Subtract180Longitude()

        results = list(subtract.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile_after = results[0]
        longitudes_after = from_shaped_array(nexus_tile_after.tile.swath_tile.longitude)

        self.assertTrue(np.all(np.equal(longitudes_before, longitudes_after)))

    def test_subtraction_longitudes_greater_than_180(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascat_longitude_more_than_180.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        nexus_tile_before = nexusproto.NexusTile.FromString(nexustile_str)
        longitudes_before = from_shaped_array(nexus_tile_before.tile.swath_tile.longitude)

        subtract = sdap.processors.Subtract180Longitude()

        results = list(subtract.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile_after = results[0]
        longitudes_after = from_shaped_array(nexus_tile_after.tile.swath_tile.longitude)

        self.assertTrue(np.all(np.not_equal(longitudes_before, longitudes_after)))
        self.assertTrue(np.all(longitudes_after[longitudes_after < 0]))
        self.assertAlmostEqual(-96.61, longitudes_after[0][26], places=2)
