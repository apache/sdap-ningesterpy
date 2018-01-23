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


import logging
import os
from subprocess import call

from processors import Processor


class CallNcpdq(Processor):

    def __init__(self, dimension_order, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dimension_order = dimension_order
        self.output_prefix = self.environ.get("OUTPUT_PREFIX", 'permuted_')
        self.permute_variable = self.environ["PERMUTE_VARIABLE"]

    def process(self, input_data):
        """
        input_data: Path to input netCDF file

        If environment variable `PERMUTE_VARIABLE` is not set:
            Calls ``ncpdq -a ${DIMENSION_ORDER} in_path ${OUTPUT_PREFIX}in_path``
        Otherwise:
            Calls ``ncpdq -v ${PERMUTE_VARIABLE} -a ${DIMENSION_ORDER} in_path ${OUTPUT_PREFIX}in_path``
        """

        output_filename = self.output_prefix + os.path.basename(input_data)
        output_path = os.path.join(os.path.dirname(input_data), output_filename)

        command = ['ncpdq', '-a', ','.join(self.dimension_order)]

        if self.permute_variable:
            command.append('-v')
            command.append(self.permute_variable)

        command.append(input_data)
        command.append(output_path)

        logging.debug('Calling command %s' % ' '.join(command))
        retcode = call(command)
        logging.debug('Command returned exit code %d' % retcode)

        yield output_path
