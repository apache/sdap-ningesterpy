"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from os import path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array

import processors


class TestAvhrrData(unittest.TestCase):
    def setUp(self):
        self.module = processors.KelvinToCelsius()

    def test_kelvin_to_celsius(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'avhrr_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        nexus_tile_before = nexusproto.NexusTile.FromString(nexustile_str)
        sst_before = from_shaped_array(nexus_tile_before.tile.grid_tile.variable_data)

        results = list(self.module.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile_after = results[0]
        sst_after = from_shaped_array(nexus_tile_after.tile.grid_tile.variable_data)

        # Just spot check a couple of values
        expected_sst = np.subtract(sst_before[0][0][0], np.float32(273.15))
        self.assertEqual(expected_sst, sst_after[0][0][0])

        expected_sst = np.subtract(sst_before[0][9][9], np.float32(273.15))
        self.assertEqual(expected_sst, sst_after[0][9][9])
