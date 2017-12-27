"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from os import path

import numpy as np
from nexusproto.serialization import from_shaped_array

import processors


class TestAscatbUData(unittest.TestCase):

    def test_u_conversion(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        converter = processors.WindDirSpeedToUV('U')

        results = list(converter.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile = results[0]

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('swath_tile'))

        # Check data
        tile_data = np.ma.masked_invalid(from_shaped_array(nexus_tile.tile.swath_tile.variable_data))
        self.assertEqual(82, np.ma.count(tile_data))

        # Check meta data
        meta_list = nexus_tile.tile.swath_tile.meta_data
        self.assertEqual(3, len(meta_list))
        wind_dir = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_dir')
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_dir.meta_data)).shape)
        self.assertIsNotNone(wind_dir)
        wind_speed = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_speed')
        self.assertIsNotNone(wind_speed)
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_speed.meta_data)).shape)
        wind_v = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_v')
        self.assertIsNotNone(wind_v)
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_v.meta_data)).shape)


class TestAscatbVData(unittest.TestCase):

    def test_u_conversion(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        with open(test_file, 'rb') as f:
            nexustile_str = f.read()

        converter = processors.WindDirSpeedToUV('V')

        results = list(converter.process(nexustile_str))

        self.assertEqual(1, len(results))

        nexus_tile = results[0]

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('swath_tile'))

        # Check data
        tile_data = np.ma.masked_invalid(from_shaped_array(nexus_tile.tile.swath_tile.variable_data))
        self.assertEqual(82, np.ma.count(tile_data))

        # Check meta data
        meta_list = nexus_tile.tile.swath_tile.meta_data
        self.assertEqual(3, len(meta_list))
        wind_dir = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_dir')
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_dir.meta_data)).shape)
        self.assertIsNotNone(wind_dir)
        wind_speed = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_speed')
        self.assertIsNotNone(wind_speed)
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_speed.meta_data)).shape)
        wind_u = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_u')
        self.assertIsNotNone(wind_u)
        self.assertEqual(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_u.meta_data)).shape)


if __name__ == '__main__':
    unittest.main()
