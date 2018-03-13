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

import nexusproto.DataTile_pb2
from netCDF4 import Dataset

from sdap.processors import NexusTileProcessor


class PromoteVariableToGlobalAttribute(NexusTileProcessor):

    def __init__(self, attribute_name, variable_name, dimensioned_by, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.attribute_name = attribute_name
        self.variable_name = variable_name
        self.dimensioned_by = dimensioned_by

    def process_nexus_tile(self, nexus_tile):
        output_tile = nexusproto.DataTile_pb2.NexusTile()
        output_tile.CopyFrom(nexus_tile)

        file_path = output_tile.summary.granule
        file_path = file_path[len('file:'):] if file_path.startswith('file:') else file_path

        dimtoslice = {}
        for dimension in output_tile.summary.section_spec.split(','):
            name, start, stop = dimension.split(':')
            dimtoslice[name] = slice(int(start), int(stop))

        with Dataset(file_path) as ds:
            new_attr = output_tile.summary.global_attributes.add()
            new_attr.name = self.attribute_name
            new_attr.values.extend(
                [str(v) for v in ds[self.variable_name][[dimtoslice[dim] for dim in self.dimensioned_by]]])

        yield output_tile
