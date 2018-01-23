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
from datetime import datetime

import numpy as np
from netCDF4 import Dataset
from pytz import timezone
from scipy import interpolate

from processors import Processor

UTC = timezone('UTC')
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


class Regrid1x1(Processor):

    def __init__(self, variables_to_regrid, latitude_var_name, longitude_var_name, time_var_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.variables_to_regrid = variables_to_regrid
        self.latitude_var_name = latitude_var_name
        self.longitude_var_name = longitude_var_name
        self.time_var_name = time_var_name

        self.filename_prefix = self.environ.get("FILENAME_PREFIX", '1x1regrid-')

        vvr = self.environ['VARIABLE_VALID_RANGE']
        if vvr:
            vvr_iter = iter(vvr.split(':'))
            self.variable_valid_range = {varrange[0]: [varrange[1], varrange[2]] for varrange in
                                         zip(vvr_iter, vvr_iter, vvr_iter)}
        else:
            self.variable_valid_range = {}

    def process(self, in_filepath):
        in_path = os.path.join('/', *in_filepath.split(os.sep)[0:-1])
        out_filepath = os.path.join(in_path, self.filename_prefix + in_filepath.split(os.sep)[-1])

        with Dataset(in_filepath) as inputds:
            in_lon = inputds[self.longitude_var_name]
            in_lat = inputds[self.latitude_var_name]
            in_time = inputds[self.time_var_name]

            lon1deg = np.arange(np.floor(np.min(in_lon)), np.ceil(np.max(in_lon)), 1)
            lat1deg = np.arange(np.floor(np.min(in_lat)), np.ceil(np.max(in_lat)), 1)
            out_time = np.array(in_time)

            with Dataset(out_filepath, mode='w') as outputds:
                outputds.createDimension(self.longitude_var_name, len(lon1deg))
                outputds.createVariable(self.longitude_var_name, in_lon.dtype, dimensions=(self.longitude_var_name,))
                outputds[self.longitude_var_name][:] = lon1deg
                outputds[self.longitude_var_name].setncatts(
                    {attrname: inputds[self.longitude_var_name].getncattr(attrname) for attrname in
                     inputds[self.longitude_var_name].ncattrs() if
                     str(attrname) not in ['bounds', 'valid_min', 'valid_max']})

                outputds.createDimension(self.latitude_var_name, len(lat1deg))
                outputds.createVariable(self.latitude_var_name, in_lat.dtype, dimensions=(self.latitude_var_name,))
                outputds[self.latitude_var_name][:] = lat1deg
                outputds[self.latitude_var_name].setncatts(
                    {attrname: inputds[self.latitude_var_name].getncattr(attrname) for attrname in
                     inputds[self.latitude_var_name].ncattrs() if
                     str(attrname) not in ['bounds', 'valid_min', 'valid_max']})

                outputds.createDimension(self.time_var_name)
                outputds.createVariable(self.time_var_name, inputds[self.time_var_name].dtype,
                                        dimensions=(self.time_var_name,))
                outputds[self.time_var_name][:] = out_time
                outputds[self.time_var_name].setncatts(
                    {attrname: inputds[self.time_var_name].getncattr(attrname) for attrname in
                     inputds[self.time_var_name].ncattrs()
                     if
                     str(attrname) != 'bounds'})

                for variable_name in self.variables_to_regrid.split(','):

                    # If longitude is the first dimension, we need to transpose the dimensions
                    transpose_dimensions = inputds[variable_name].dimensions == (
                        self.time_var_name, self.longitude_var_name, self.latitude_var_name)

                    outputds.createVariable(variable_name, inputds[variable_name].dtype,
                                            dimensions=inputds[variable_name].dimensions)
                    outputds[variable_name].setncatts(
                        {attrname: inputds[variable_name].getncattr(attrname) for attrname in
                         inputds[variable_name].ncattrs()})
                    if variable_name in self.variable_valid_range.keys():
                        outputds[variable_name].valid_range = [
                            np.array([self.variable_valid_range[variable_name][0]],
                                     dtype=inputds[variable_name].dtype).item(),
                            np.array([self.variable_valid_range[variable_name][1]],
                                     dtype=inputds[variable_name].dtype).item()]

                    for ti in range(0, len(out_time)):
                        in_data = inputds[variable_name][ti, :, :]
                        if transpose_dimensions:
                            in_data = in_data.T

                        # Produces erroneous values on the edges of data
                        # interp_func = interpolate.interp2d(in_lon[:], in_lat[:], in_data[:], fill_value=float('NaN'))

                        x_mesh, y_mesh = np.meshgrid(in_lon[:], in_lat[:], copy=False)

                        # Does not work for large datasets (n > 5000)
                        # interp_func = interpolate.Rbf(x_mesh, y_mesh, in_data[:], function='linear', smooth=0)

                        x1_mesh, y1_mesh = np.meshgrid(lon1deg, lat1deg, copy=False)
                        out_data = interpolate.griddata(np.array([x_mesh.ravel(), y_mesh.ravel()]).T, in_data.ravel(),
                                                        (x1_mesh, y1_mesh), method='nearest')

                        if transpose_dimensions:
                            out_data = out_data.T

                        outputds[variable_name][ti, :] = out_data[np.newaxis, :]

                global_atts = {
                    'geospatial_lon_min': np.float(np.min(lon1deg)),
                    'geospatial_lon_max': np.float(np.max(lon1deg)),
                    'geospatial_lat_min': np.float(np.min(lat1deg)),
                    'geospatial_lat_max': np.float(np.max(lat1deg)),
                    'Conventions': 'CF-1.6',
                    'date_created': datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601),
                    'title': getattr(inputds, 'title', ''),
                    'time_coverage_start': getattr(inputds, 'time_coverage_start', ''),
                    'time_coverage_end': getattr(inputds, 'time_coverage_end', ''),
                    'Institution': getattr(inputds, 'Institution', ''),
                    'summary': getattr(inputds, 'summary', ''),
                }

                outputds.setncatts(global_atts)

        yield out_filepath
