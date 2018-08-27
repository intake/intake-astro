from collections import OrderedDict
from intake.source.base import DataSource, Schema
from . import __version__


class FITSTableSource(DataSource):
    name = 'fits_table'
    container = 'dataframe'
    version = __version__
    partition_access = True

    def __init__(self, url, ext=0, chunksize=None, storage_options=None,
                 metadata=None):
        super(FITSTableSource, self).__init__(metadata)
        self.url = url
        self.ext = ext
        self.chunks = chunksize
        self.storage_options = storage_options or {}
        self.df = None
        self.files = None

    def _get_schema(self):
        from dask.bytes import open_files
        import dask.dataframe as dd
        from dask.base import tokenize
        import dask
        if self.df is None:
            self.files = open_files(self.url, **self.storage_options)
            name = 'fits-table-' + tokenize(self.url, self.chunks, self.ext)
            dpart = dask.delayed(_get_fits_section)
            parts = []
            dtype = None
            for part in self.files:
                if self.chunks:
                    header, dtype, shape = _get_fits_header(part, self.ext)
                    l = shape[0]
                    for start in range(0, l, self.chunks):
                        section = (start, min(start + self.chunks, l))
                        parts.append(dpart(part, self.ext, section))
                else:
                    if dtype is None:
                        header, dtype, shape = _get_fits_header(part, self.ext)
                    parts.append(dpart(part, self.ext, None))
            self.header, self.dtype, self.shape = header, dtype, shape

            self.df = dd.from_delayed(parts, prefix=name)
            self._schema = Schema(
                dtype=self.dtype,
                shape=self.shape,
                extra_metadata=dict(self.header.items()),
                npartitions=self.df.npartitions
            )
        return self._schema

    def to_dask(self):
        self._get_schema()
        return self.df

    def read_chunked(self):
        return self.to_dask()

    def read_partition(self, i):
        self._get_schema()
        return self.df.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self.df.compute()

    def _close(self):
        self.df = None
        self.files = None


def _get_fits_section(fn, ext=0, section=None):
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
            from astropy.io.fits.hdu.table import _FormatP, _FormatQ
            # copied from hdu._get_tbdata()
            hdus = open(f)
            hdu = hdus[ext]
            if (any(type(r) in (_FormatP, _FormatQ)
                    for r in hdu.columns._recformats) and
                    hdu._data_size is not None and
                    hdu._data_size > hdu._theap):
                raise ValueError('Can only read sections from tables without '
                                 'heap; use section=None')
            data_offset = hdu._data_offset + hdu.columns.dtype.itemsize * start
            raw_data = hdu._get_raw_data((end - start), hdu.columns.dtype,
                                         data_offset)
            data = raw_data.view(np.rec.recarray)
            hdu._init_tbdata(data)
            data = data.view(hdu._data_type)

            # do in-place byteswapping (required for pandas to use data)
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


def _get_fits_header(fn, ext=0):
    with fn as f:
        from astropy.io.fits import open
        hdu = open(f)[ext]
        return hdu.header, hdu.columns.dtype.descr, (hdu._nrows,
                                                     len(hdu.columns))
