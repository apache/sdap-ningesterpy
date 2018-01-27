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

import subprocess
import unittest
from os import path, remove

from netCDF4 import Dataset

from ningesterpy.processors import callncpdq


class TestMeasuresData(unittest.TestCase):

    @unittest.skipIf(int(subprocess.call(["ncpdq", "-r"])) not in {0, 1}, "requires ncpdq")
    def test_permute_all_variables(self):
        dimension_order = ['Time', 'Latitude', 'Longitude']

        the_module = callncpdq.CallNcpdq(dimension_order)

        expected_dimensions = dimension_order

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')

        output_path = list(the_module.process(test_file))[0]

        with Dataset(output_path) as ds:
            sla_var = ds['SLA']
            actual_dimensions = [str(dim) for dim in sla_var.dimensions]

        remove(output_path)
        self.assertEqual(expected_dimensions, actual_dimensions)

    @unittest.skipIf(int(subprocess.call(["ncpdq", "-r"])) not in {0, 1}, "requires ncpdq")
    def test_permute_one_variable(self):
        dimension_order = ['Time', 'Latitude', 'Longitude']
        permute_var = 'SLA'

        the_module = callncpdq.CallNcpdq(dimension_order, environ={"PERMUTE_VARIABLE": permute_var})

        expected_dimensions = dimension_order

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')

        output_path = list(the_module.process(test_file))[0]

        with Dataset(output_path) as ds:
            sla_var = ds[permute_var]
            actual_dimensions = [str(dim) for dim in sla_var.dimensions]

        remove(output_path)
        self.assertEqual(expected_dimensions, actual_dimensions)
