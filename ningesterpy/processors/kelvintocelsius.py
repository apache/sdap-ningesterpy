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


from nexusproto.serialization import from_shaped_array, to_shaped_array

from ningesterpy.processors import NexusTileProcessor


class KelvinToCelsius(NexusTileProcessor):
    def process_nexus_tile(self, nexus_tile):
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        var_data = from_shaped_array(the_tile_data.variable_data) - 273.15

        the_tile_data.variable_data.CopyFrom(to_shaped_array(var_data))

        yield nexus_tile
