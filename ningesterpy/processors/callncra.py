"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import glob
import os
from subprocess import call

from netCDF4 import Dataset, num2date

from processors import Processor


class CallNcra(Processor):
    def __init__(self, output_filename_pattern, time_var_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.output_filename_pattern = output_filename_pattern
        self.time_var_name = time_var_name

        self.glob_pattern = self.environ.get("FILEMATCH_PATTERN", '*.nc')

    def process(self, in_path):
        target_datetime = self.get_datetime_from_dataset(in_path)
        target_yearmonth = target_datetime.strftime('%Y%m')

        output_filename = target_datetime.strftime(self.output_filename_pattern)
        output_path = os.path.join(os.path.dirname(in_path), output_filename)

        datasets = glob.glob(os.path.join(os.path.dirname(in_path), self.glob_pattern))

        datasets_to_average = [dataset_path for dataset_path in datasets if
                               self.get_datetime_from_dataset(dataset_path).strftime('%Y%m') == target_yearmonth]

        command = ['ncra', '-O']
        command.extend(datasets_to_average)
        command.append(output_path)
        call(command)

        yield output_path

    def get_datetime_from_dataset(self, dataset_path):
        with Dataset(dataset_path) as dataset_in:
            time_units = getattr(dataset_in[self.time_var_name], 'units', None)
            calendar = getattr(dataset_in[self.time_var_name], 'calendar', 'standard')
            thedatetime = num2date(dataset_in[self.time_var_name][:].item(), units=time_units, calendar=calendar)
        return thedatetime
