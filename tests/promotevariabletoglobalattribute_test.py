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

import sdap.processors
import nexusproto.DataTile_pb2


class TestReadWSWMData(unittest.TestCase):

    def test_read_not_empty_wswm(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_wswm.nc')

        promoter = sdap.processors.PromoteVariableToGlobalAttribute('rivid_i', 'rivid', ('rivid',))

        input_tile = nexusproto.DataTile_pb2.NexusTile()
        tile_summary = nexusproto.DataTile_pb2.TileSummary()
        tile_summary.granule = "file:%s" % test_file
        tile_summary.section_spec = "time:0:5832,rivid:0:1"
        input_tile.summary.CopyFrom(tile_summary)

        results = list(promoter.process(input_tile))

        print(results)