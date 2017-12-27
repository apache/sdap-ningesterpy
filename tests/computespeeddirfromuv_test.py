"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from os import path

import numpy as np
from nexusproto.serialization import from_shaped_array

import processors
from processors.computespeeddirfromuv import calculate_speed_direction


class TestConversion(unittest.TestCase):
    def test_dir_from_north(self):
        # Negative v means wind is wind blowing to the South
        u = 0
        v = -1

        speed, direction = calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from North (0 degrees)
        self.assertEqual(0, direction)

    def test_dir_from_east(self):
        # Negative u means wind is blowing to the West
        u = -1
        v = 0

        speed, direction = calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from East (90 degrees)
        self.assertEqual(90, direction)

    def test_dir_from_south(self):
        # Positive v means wind is blowing to the North
        u = 0
        v = 1

        speed, direction = calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from South (180 degrees)
        self.assertEqual(180, direction)

    def test_dir_from_west(self):
        # Positive u means wind is blowing to the East
        u = 1
        v = 0

        speed, direction = calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from West (270 degrees)
        self.assertEqual(270, direction)

    def test_speed(self):
        # Speed is simply sqrt(u^2 + v^2)
        u = 2
        v = 2

        speed, direction = calculate_speed_direction(u, v)

        self.assertAlmostEqual(2.8284271, speed)
        # Wind should be from the southwest
        self.assertTrue(180 < direction < 270)


class TestCcmpData(unittest.TestCase):
    def setUp(self):
        self.module = processors.ComputeSpeedDirFromUV('uwnd', 'vwnd')

    def test_speed_dir_computation(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ccmp_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        results = list(self.module.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile = results[0]

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('grid_tile'))

        # Check data
        tile_data = np.ma.masked_invalid(from_shaped_array(nexus_tile.tile.grid_tile.variable_data))
        self.assertEqual(3306, np.ma.count(tile_data))

        # Check meta data
        meta_list = nexus_tile.tile.grid_tile.meta_data
        self.assertEqual(3, len(meta_list))
        wind_dir = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_dir')
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_dir.meta_data)).shape)
        self.assertIsNotNone(wind_dir)
        wind_speed = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_speed')
        self.assertIsNotNone(wind_speed)
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_speed.meta_data)).shape)
        wind_v = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'vwnd')
        self.assertIsNotNone(wind_v)
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_v.meta_data)).shape)


if __name__ == '__main__':
    unittest.main()
