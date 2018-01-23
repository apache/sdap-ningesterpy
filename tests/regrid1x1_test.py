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

import os
import unittest

import processors


def delete_file_if_exists(filename):
    try:
        os.remove(filename)
    except OSError:
        pass


class TestSSHData(unittest.TestCase):

    def setUp(self):
        self.test_file = os.path.join(os.path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')
        self.prefix = 'test-regrid'
        self.expected_output_path = os.path.join(os.path.dirname(self.test_file),
                                                 self.prefix + os.path.basename(self.test_file))
        delete_file_if_exists(self.expected_output_path)

    def tearDown(self):
        delete_file_if_exists(self.expected_output_path)

    def test_ssh_grid(self):
        regridder = processors.Regrid1x1('SLA', 'Latitude', 'Longitude', 'Time',
                                         variable_valid_range='SLA:-100.0:100.0:SLA_ERR:-5000:5000',
                                         filename_prefix=self.prefix)

        results = list(regridder.process(self.test_file))

        self.assertEqual(1, len(results))


class TestGRACEData(unittest.TestCase):
    def setUp(self):
        self.test_file = ''  # os.path.join(os.path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')
        self.prefix = 'test-regrid'
        self.expected_output_path = os.path.join(os.path.dirname(self.test_file),
                                                 self.prefix + os.path.basename(self.test_file))
        delete_file_if_exists(self.expected_output_path)

    def tearDown(self):
        delete_file_if_exists(self.expected_output_path)

    @unittest.skip
    def test_lwe_grid(self):
        regridder = processors.Regrid1x1('lwe_thickness', 'lat', 'lon', 'tim',
                                         filename_prefix=self.prefix)

        results = list(regridder.process(self.test_file))

        self.assertEqual(1, len(results))


class TestIceShelfData(unittest.TestCase):
    def setUp(self):
        self.test_file = ''  # os.path.join(os.path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')
        self.prefix = 'test-regrid'
        self.expected_output_path = os.path.join(os.path.dirname(self.test_file),
                                                 self.prefix + os.path.basename(self.test_file))
        delete_file_if_exists(self.expected_output_path)

    def tearDown(self):
        delete_file_if_exists(self.expected_output_path)

    @unittest.skip
    def test_height_raw(self):
        regridder = processors.Regrid1x1('height_raw,height_filt,height_err', 'lat', 'lon', 'tim',
                                         filename_prefix=self.prefix)

        results = list(regridder.process(self.test_file))

        self.assertEqual(1, len(results))
