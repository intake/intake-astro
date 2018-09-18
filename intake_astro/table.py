from collections import OrderedDict
import copy
from intake.source.base import DataSource, Schema
from . import __version__


class FITSTableSource(DataSource):
    """Read FITS tabular data into dataframes

    For one or more FITS files, which can be local or remote, with support
    for partitioning within files.

    Parameters
    ----------
    url: str or list of str
        files to load. Can include protocol specifiers and/or glob
        characters
    ext: str or int
        Extension to load. Normally 0 or 1.
    chunksize: int or None
        For partitioning within files, use this many rows per partition.
        This is very inefficient for compressed files, and for remote
        files, will require at least touching each file to discover the
        number of rows, before even starting to read the data. Cannot be
        used with FITS tables with a "heap", i.e., containing variable-
        length arrays.
    storage_options: dict or None
        Additional keyword arguments to pass to the storage back-end.
    metadata:
        Arbitrary information to associate with this source.

    After reading the schema, the source will have attributes:
    ``header`` - the full FITS header of one of the files as a dict,
    ``dtype`` - a numpy-like list of field/dtype string pairs,
    ``shape`` - where the number of rows will only be known if using
    partitioning or for a single file input.
    """
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
            length = 0
            for part in self.files:
                if self.chunks:
                    header, dtype, shape = _get_fits_header(part, self.ext)
                    l = shape[0]
                    for start in range(0, l, self.chunks):
                        section = (start, min(start + self.chunks, l))
                        parts.append(dpart(part, self.ext, section))
                    length += l
                else:
                    if dtype is None:
                        header, dtype, shape = _get_fits_header(part, self.ext)
                        if len(self.files) == 1:
                            # if not sectioning, we don't try to find the total
                            # number of rows, so we only know this for exactly
                            # one file
                            length = shape[0]
                    parts.append(dpart(part, self.ext, None))
            self.header, self.dtype, self.shape = header, dtype, (
                (length or None), shape[1])

            self.df = dd.from_delayed(parts, prefix=name, meta=dtype)
            self._schema = Schema(
                dtype=self.dtype,
                shape=self.shape,
                extra_metadata=self.header,
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
    with copy.copy(fn) as f:
        if section is None:
            from astropy.table import Table
            t = Table.read(f, hdu=ext, format='fits')
            return t.to_pandas()
        else:
            start, end = section
            from astropy.io.fits import open
            from astropy.io.fits.hdu.table import _FormatP, _FormatQ
            # copied from hdu._get_tbdata()
            hdus = open(f, memmap=False, cache=False)
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
            data._coldefs = hdu.columns
            data._character_as_bytes = hdu._character_as_bytes

            # byteswapping by copy
            out = np.empty(len(data), dtype=_dtypes(hdu))
            for col in out.dtype.fields:
                out[col][:] = data[col]
            df = pd.DataFrame(out)
            return df


def _get_fits_header(fn, ext=0):
    with copy.copy(fn) as f:
        from astropy.io.fits import open
        hdu = open(f, memmap=False, cache=False)[ext]
        return dict(hdu.header.items()), _dtypes(hdu), (
            hdu._nrows, len(hdu.columns))


def _dtypes(hdu):
    dt = hdu.columns.dtype.newbyteorder('=').descr
    dt2 = []
    for i, (field, d) in enumerate(dt):
        # TODO: may have other special types, perhaps in special function
        if hdu.header['TFORM%i' % (i + 1)] == "L":
            dt2.append((field, '|b1'))
        elif hdu.columns[field].bzero == 32768:
            # convention for unsigned type
            dt2.append((field, d.replace('i', 'u')))
        else:
            dt2.append((field, d))
    return dt2
