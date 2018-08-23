from collections import OrderedDict
from intake.source.base import DataSource, Schema
from . import __version__


class FITSTableSource(DataSource):
    name = 'fits_array'
    container = 'dataframe'
    version = __version__
    partition_access = True


def _get_section(fn, ext=0, section=None):
    import numpy as np
    import pandas as pd
    with fn as f:
        if section is None:
            from astropy.table import Table
            t = Table.read(f, hdu=ext, format='fits')
            return t.to_pandas()
        else:
            start, end = section
            from astropy.io.fits import open
            # copied from hdu._get_tbdata()
            # TODO: should be made to fail for files with heap
            hdus = open(f)
            hdu = hdus[ext]
            data_offset = hdu._data_offset + hdu.columns.dtype.itemsize * start
            raw_data = hdu._get_raw_data((end - start), hdu.columns.dtype,
                                         data_offset)
            data = raw_data.view(np.rec.recarray)
            hdu._init_tbdata(data)
            data = data.view(hdu._data_type)

            # do byteswapping (required for pandas to use data)
            dtypes = OrderedDict(data.dtype.fields)
            dt2 = []
            for col, val in dtypes.items():
                if not val[0].isnative:
                    dt = val[0].newbyteorder()
                    dtypes[col] = dt, 1
                    data[col][:] = np.frombuffer(data[col].tobytes(), dtype=dt)
                    dt2.append((col, dt))
                else:
                    dt2.append((col, val[0]))
            df = pd.DataFrame(np.frombuffer(data.data, dtype=dt2))
            return df
