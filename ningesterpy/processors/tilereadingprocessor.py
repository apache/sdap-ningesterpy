"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import datetime
from collections import OrderedDict
from contextlib import contextmanager
from os import sep, path, remove
from urllib.request import urlopen

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from netCDF4 import Dataset, num2date
from nexusproto.serialization import to_shaped_array, to_metadata
from pytz import timezone

from processors import Processor

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))


@contextmanager
def closing(thing):
    try:
        yield thing
    finally:
        thing.close()


def parse_input(the_input, temp_dir):
    # Split string on ';'
    specs_and_path = [str(part).strip() for part in str(the_input).split(';')]

    # Tile specifications are all but the last element
    specs = specs_and_path[:-1]
    # Generate a list of tuples, where each tuple is a (string, map) that represents a
    # tile spec in the form (str(section_spec), { dimension_name : slice, dimension2_name : slice })
    tile_specifications = [slices_from_spec(section_spec) for section_spec in specs]

    # The path is the last element of the input split by ';'
    file_path = specs_and_path[-1]
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
    file_path = file_path[len('file://'):] if file_path.startswith('file://') else file_path

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


def new_nexus_tile(file_path, section_spec):
    nexus_tile = nexusproto.NexusTile()
    tile_summary = nexusproto.TileSummary()
    tile_summary.granule = file_path.split(sep)[-1]
    tile_summary.section_spec = section_spec
    nexus_tile.summary.CopyFrom(tile_summary)
    return nexus_tile


class TileReadingProcessor(Processor):
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

    def process(self, input_data):
        tile_specifications, file_path = parse_input(input_data, self.temp_dir)

        for data in self.read_data(tile_specifications, file_path):
            yield data

        # If temp dir is defined, delete the temporary file
        if self.temp_dir is not None:
            remove(file_path)

    def read_data(self, tile_specifications, file_path):
        raise NotImplementedError


class GridReadingProcessor(TileReadingProcessor):
    def read_data(self, tile_specifications, file_path):
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

                nexus_tile = new_nexus_tile(file_path, section_spec)
                nexus_tile.tile.grid_tile.CopyFrom(tile)

                yield nexus_tile


class SwathReadingProcessor(TileReadingProcessor):
    def __init__(self, variable_to_read, latitude, longitude, time, **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        # Time is required for swath data
        self.time = time

    def read_data(self, tile_specifications, file_path):
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

                nexus_tile = new_nexus_tile(file_path, section_spec)
                nexus_tile.tile.swath_tile.CopyFrom(tile)

                yield nexus_tile


class TimeSeriesReadingProcessor(TileReadingProcessor):
    def __init__(self, variable_to_read, latitude, longitude, time, **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        # Time is required for swath data
        self.time = time

    def read_data(self, tile_specifications, file_path):
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

                timevar = ds[self.time]
                # Note assumption is that index of time is start value in dimtoslice
                tile.time = to_seconds_from_epoch(timevar[dimtoslice[self.time].start],
                                                  timeunits=timevar.getncattr('units'),
                                                  timeoffset=self.time_offset)

                nexus_tile = new_nexus_tile(file_path, section_spec)
                nexus_tile.tile.time_series_tile.CopyFrom(tile)

                yield nexus_tile
