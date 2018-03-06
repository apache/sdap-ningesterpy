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

import datetime
from collections import OrderedDict
from contextlib import contextmanager
from os import sep, path, remove
from urllib.request import urlopen

from nexusproto import DataTile_pb2 as nexusproto
import numpy
from netCDF4 import Dataset, num2date
from nexusproto.serialization import to_shaped_array, to_metadata
from pytz import timezone

from sdap.processors import NexusTileProcessor

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))


@contextmanager
def closing(thing):
    try:
        yield thing
    finally:
        thing.close()


def parse_input(the_input_tile, temp_dir):
    specs = [the_input_tile.summary.section_spec]
    # Generate a list of tuples, where each tuple is a (string, map) that represents a
    # tile spec in the form (str(section_spec), { dimension_name : slice, dimension2_name : slice })
    tile_specifications = [slices_from_spec(section_spec) for section_spec in specs]

    file_path = the_input_tile.summary.granule
    file_name = file_path.split(sep)[-1]
    # If given a temporary directory location, copy the file to the temporary directory and return that path
    if temp_dir is not None:
        temp_file_path = path.join(temp_dir, file_name)
        with closing(urlopen(file_path)) as original_granule:
            with open(temp_file_path, 'wb') as temp_granule:
                for chunk in iter((lambda: original_granule.read(512000)), ''):
                    temp_granule.write(chunk)

                file_path = temp_file_path

    # Remove file:// if it's there because netcdf lib doesn't like it
    file_path = file_path[len('file:'):] if file_path.startswith('file:') else file_path

    return tile_specifications, file_path


def slices_from_spec(spec):
    dimtoslice = {}
    for dimension in spec.split(','):
        name, start, stop = dimension.split(':')
        dimtoslice[name] = slice(int(start), int(stop))

    return spec, dimtoslice


def to_seconds_from_epoch(date, timeunits=None, start_day=None, timeoffset=None):
    try:
        date = num2date(date, units=timeunits)
    except ValueError:
        assert isinstance(start_day, datetime.date), "start_day is not a datetime.date object"
        the_datetime = datetime.datetime.combine(start_day, datetime.datetime.min.time())
        date = the_datetime + datetime.timedelta(seconds=date)

    if isinstance(date, datetime.datetime):
        date = timezone('UTC').localize(date)
    else:
        date = timezone('UTC').localize(datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S'))

    if timeoffset is not None:
        return int((date - EPOCH).total_seconds()) + timeoffset
    else:
        return int((date - EPOCH).total_seconds())


def get_ordered_slices(ds, variable, dimension_to_slice):
    dimensions_for_variable = [str(dimension) for dimension in ds[variable].dimensions]
    ordered_slices = OrderedDict()
    for dimension in dimensions_for_variable:
        ordered_slices[dimension] = dimension_to_slice[dimension]
    return ordered_slices


class TileReadingProcessor(NexusTileProcessor):

    def __init__(self, variable_to_read, latitude, longitude, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Required properties for all reader types
        self.variable_to_read = variable_to_read
        self.latitude = latitude
        self.longitude = longitude

        # Common optional properties
        self.temp_dir = self.environ['TEMP_DIR']
        self.metadata = self.environ['META']
        self.start_of_day = self.environ['GLBLATTR_DAY']
        self.start_of_day_pattern = self.environ['GLBLATTR_DAY_FORMAT']
        self.time_offset = int(self.environ['TIME_OFFSET']) if self.environ['TIME_OFFSET'] is not None else None

    def process_nexus_tile(self, input_tile):
        tile_specifications, file_path = parse_input(input_tile, self.temp_dir)

        output_tile = nexusproto.NexusTile()
        output_tile.CopyFrom(input_tile)

        for tile in self.read_data(tile_specifications, file_path, output_tile):
            yield tile

        # If temp dir is defined, delete the temporary file
        if self.temp_dir is not None:
            remove(file_path)

    def read_data(self, tile_specifications, file_path, output_tile):
        raise NotImplementedError


class GridReadingProcessor(TileReadingProcessor):
    def read_data(self, tile_specifications, file_path, output_tile):
        # Time is optional for Grid data
        time = self.environ['TIME']

        with Dataset(file_path) as ds:
            for section_spec, dimtoslice in tile_specifications:
                tile = nexusproto.GridTile()

                tile.latitude.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.latitude][dimtoslice[self.latitude]], numpy.NaN)))

                tile.longitude.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.longitude][dimtoslice[self.longitude]], numpy.NaN)))

                # Before we read the data we need to make sure the dimensions are in the proper order so we don't have any
                #  indexing issues
                ordered_slices = get_ordered_slices(ds, self.variable_to_read, dimtoslice)
                # Read data using the ordered slices, replacing masked values with NaN
                data_array = numpy.ma.filled(ds[self.variable_to_read][tuple(ordered_slices.values())], numpy.NaN)

                tile.variable_data.CopyFrom(to_shaped_array(data_array))

                if self.metadata is not None:
                    tile.meta_data.add().CopyFrom(
                        to_metadata(self.metadata, ds[self.metadata][tuple(ordered_slices.values())]))

                if time is not None:
                    timevar = ds[time]
                    # Note assumption is that index of time is start value in dimtoslice
                    tile.time = to_seconds_from_epoch(timevar[dimtoslice[time].start],
                                                      timeunits=timevar.getncattr('units'),
                                                      timeoffset=self.time_offset)

                output_tile.tile.grid_tile.CopyFrom(tile)

                yield output_tile


class SwathReadingProcessor(TileReadingProcessor):
    def __init__(self, variable_to_read, latitude, longitude, time, **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        # Time is required for swath data
        self.time = time

    def read_data(self, tile_specifications, file_path, output_tile):
        with Dataset(file_path) as ds:
            for section_spec, dimtoslice in tile_specifications:
                tile = nexusproto.SwathTile()
                # Time Lat Long Data and metadata should all be indexed by the same dimensions, order the incoming spec once using the data variable
                ordered_slices = get_ordered_slices(ds, self.variable_to_read, dimtoslice)
                tile.latitude.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.latitude][tuple(ordered_slices.values())], numpy.NaN)))

                tile.longitude.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.longitude][tuple(ordered_slices.values())], numpy.NaN)))

                timetile = ds[self.time][
                    tuple([ordered_slices[time_dim] for time_dim in ds[self.time].dimensions])].astype(
                    'float64',
                    casting='same_kind',
                    copy=False)
                timeunits = ds[self.time].getncattr('units')
                try:
                    start_of_day_date = datetime.datetime.strptime(ds.getncattr(self.start_of_day),
                                                                   self.start_of_day_pattern)
                except Exception:
                    start_of_day_date = None

                for index in numpy.ndindex(timetile.shape):
                    timetile[index] = to_seconds_from_epoch(timetile[index].item(), timeunits=timeunits,
                                                            start_day=start_of_day_date, timeoffset=self.time_offset)

                tile.time.CopyFrom(to_shaped_array(timetile))

                # Read the data converting masked values to NaN
                data_array = numpy.ma.filled(ds[self.variable_to_read][tuple(ordered_slices.values())], numpy.NaN)
                tile.variable_data.CopyFrom(to_shaped_array(data_array))

                if self.metadata is not None:
                    tile.meta_data.add().CopyFrom(
                        to_metadata(self.metadata, ds[self.metadata][tuple(ordered_slices.values())]))

                output_tile.tile.swath_tile.CopyFrom(tile)

                yield output_tile


class TimeSeriesReadingProcessor(TileReadingProcessor):
    def __init__(self, variable_to_read, latitude, longitude, time, **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        # Time is required for swath data
        self.time = time

    def read_data(self, tile_specifications, file_path, output_tile):
        with Dataset(file_path) as ds:
            for section_spec, dimtoslice in tile_specifications:
                tile = nexusproto.TimeSeriesTile()

                instance_dimension = next(
                    iter([dim for dim in ds[self.variable_to_read].dimensions if dim != self.time]))

                tile.latitude.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.latitude][dimtoslice[instance_dimension]], numpy.NaN)))

                tile.longitude.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.longitude][dimtoslice[instance_dimension]], numpy.NaN)))

                # Before we read the data we need to make sure the dimensions are in the proper order so we don't
                # have any indexing issues
                ordered_slices = get_ordered_slices(ds, self.variable_to_read, dimtoslice)
                # Read data using the ordered slices, replacing masked values with NaN
                data_array = numpy.ma.filled(ds[self.variable_to_read][tuple(ordered_slices.values())], numpy.NaN)

                tile.variable_data.CopyFrom(to_shaped_array(data_array))

                if self.metadata is not None:
                    tile.meta_data.add().CopyFrom(
                        to_metadata(self.metadata, ds[self.metadata][tuple(ordered_slices.values())]))

                tile.time.CopyFrom(
                    to_shaped_array(numpy.ma.filled(ds[self.time][dimtoslice[self.time]], numpy.NaN)))

                output_tile.tile.time_series_tile.CopyFrom(tile)

                yield output_tile
