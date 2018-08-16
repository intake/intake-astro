from intake.source.base import DataSource, Schema
from . import __version__


class FITSTableSource(DataSource):
    name = 'fits_array'
    container = 'dataframe'
    version = __version__
    partition_access = True

