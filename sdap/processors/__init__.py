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

from collections import defaultdict

from nexusproto import DataTile_pb2 as nexusproto


class Processor(object):
    def __init__(self, *args, **kwargs):
        self.environ = defaultdict(lambda: None)
        for k, v in kwargs.items():
            self.environ[k.upper()] = v
        pass

    def process(self, input_data):
        raise NotImplementedError


class NexusTileProcessor(Processor):
    @staticmethod
    def parse_input(input_data):
        if isinstance(input_data, nexusproto.NexusTile):
            return input_data
        else:
            return nexusproto.NexusTile.FromString(input_data)

    def process(self, input_data):
        nexus_tile = self.parse_input(input_data)

        for data in self.process_nexus_tile(nexus_tile):
            yield data

    def process_nexus_tile(self, nexus_tile):
        raise NotImplementedError


# All installed processors need to be imported and added to the dict below

from sdap.processors.callncpdq import CallNcpdq
from sdap.processors.callncra import CallNcra
from sdap.processors.computespeeddirfromuv import ComputeSpeedDirFromUV
from sdap.processors.emptytilefilter import EmptyTileFilter
from sdap.processors.kelvintocelsius import KelvinToCelsius
from sdap.processors.normalizetimebeginningofmonth import NormalizeTimeBeginningOfMonth
from sdap.processors.regrid1x1 import Regrid1x1
from sdap.processors.subtract180longitude import Subtract180Longitude
from sdap.processors.tilereadingprocessor import GridReadingProcessor, SwathReadingProcessor, TimeSeriesReadingProcessor
from sdap.processors.tilesummarizingprocessor import TileSummarizingProcessor
from sdap.processors.winddirspeedtouv import WindDirSpeedToUV

INSTALLED_PROCESSORS = {
    "CallNcpdq": CallNcpdq,
    "CallNcra": CallNcra,
    "ComputeSpeedDirFromUV": ComputeSpeedDirFromUV,
    "EmptyTileFilter": EmptyTileFilter,
    "KelvinToCelsius": KelvinToCelsius,
    "NormalizeTimeBeginningOfMonth": NormalizeTimeBeginningOfMonth,
    "Regrid1x1": Regrid1x1,
    "Subtract180Longitude": Subtract180Longitude,
    "GridReadingProcessor": GridReadingProcessor,
    "SwathReadingProcessor": SwathReadingProcessor,
    "TimeSeriesReadingProcessor": TimeSeriesReadingProcessor,
    "TileSummarizingProcessor": TileSummarizingProcessor,
    "WindDirSpeedToUV": WindDirSpeedToUV
}
