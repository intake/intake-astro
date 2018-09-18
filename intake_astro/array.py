import copy
from itertools import product, accumulate
from intake.source.base import DataSource, Schema
from . import __version__


class FITSArraySource(DataSource):
    """
    Read one of more local or remote FITS files using Intake

    At initialisation (when something calls ``._get_schema()``), the
    header of the first file will be read and a delayed array constructed.
    The properties ``header, dtype, shape, wcs`` will be populated from
    that header, and no check is made to ensure that all files are
    compatible.

    Parameters
    ----------
    url: str or list of str
        Location of the data file(s). May include glob characters; may
        include protocol specifiers.
    ext: int or str or tuple
        Extension to probe. By default, is primary extension. Can either be
        an integer referring to sequence number, or an extension name. If a
        tuple like ('SCI', 2), get the second extension named 'SCI'.
    chunks: None or tuple of int
        size of blocks to use within each file; must specify all axes,
        if using. If None, each file is one partition. Do not use chunks
        for compressed data, and only use contiguous chunks for remote
        data.
    storage_options: dics
        Parameters to pass on to storage backend
    """

    name = 'fits_array'
    container = 'ndarray'
    version = __version__
    partition_access = True

    def __init__(self, url, ext=0, chunks=None, storage_options=None,
                 metadata=None):
        # TODO: implement case where set of extensions within a file is an axis
        super(FITSArraySource, self).__init__(metadata)
        self.url = url
        self.ext = ext
        self.chunks = chunks
        self.storage_options = storage_options or {}
        self.arr = None
        self.files = None

    def _get_schema(self):
        from dask.bytes import open_files
        import dask.array as da
        from dask.base import tokenize
        if self.arr is None:
            self.files = open_files(self.url, **self.storage_options)
            self.header, self.dtype, self.shape, self.wcs = _get_header(
                self.files[0], self.ext)
            name = 'fits-array-' + tokenize(self.url, self.chunks, self.ext)
            ch = self.chunks if self.chunks is not None else self.shape
            chunks = []
            for c, s in zip(ch, self.shape):
                num = s // c
                part = [c] * num
                if s % c:
                    part.append(s % c)
                chunks.append(tuple(part))
            cums = tuple((0, ) + tuple(accumulate(ch)) for ch in chunks)
            dask = {}
            if len(self.files) > 1:
                # multi-file set
                self.shape = (len(self.files), ) + self.shape
                chunks.insert(0, (1, ) * len(self.files))
                inds = tuple(range(len(ch)) for ch in chunks)
                for (fi, *bits) in product(*inds):
                    slices = tuple(slice(i[bit], i[bit + 1])
                                   for (i, bit) in zip(cums, bits))
                    dask[(name, fi) + tuple(bits)] = (
                        _get_section, self.files[fi], self.ext, slices, False
                    )
            else:
                # single-file set
                inds = tuple(range(len(ch)) for ch in chunks)
                for bits in product(*inds):
                    slices = tuple(slice(i[bit], i[bit+1])
                                   for (i, bit) in zip(cums, bits))
                    dask[(name,) + bits] = (
                        _get_section, self.files[0], self.ext, slices, True
                    )
            self.arr = da.Array(dask, name, chunks, self.dtype, self.shape)
            self._schema = Schema(
                dtype=self.dtype,
                shape=self.shape,
                extra_metadata=dict(self.header.items()),
                npartitions=self.arr.npartitions,
                chunks=self.arr.chunks
            )
        return self._schema

    def to_dask(self):
        self._get_schema()
        return self.arr

    def read_chunked(self):
        return self.to_dask()

    def read_partition(self, i):
        self._get_schema()
        return self.arr.blocks[i].compute()

    def read(self):
        self._get_schema()
        return self.arr.compute()

    def _close(self):
        self.arr = None
        self.files = None


def _get_section(fn, ext=0, section=None, onefile=False):
    """Load data from delayed file

    Parameters
    ----------
    fn: file
    section: None or tuple of slices
        Data piece to retrieve; if None, gets everything.
    onefile: bool
        Whether this is part of a one-file set or not; if not, prepend extra
        unit axis.
    """
    from astropy.io import fits
    import numpy as np
    with copy.copy(fn) as fi:
        with fits.open(fi, memmap=False, cache=False) as hdul:
            if section is None:
                out = hdul[ext].data
            else:
                out = hdul[ext].section[section]
        if onefile:
            return out
        else:
            return out[np.newaxis, :]


def _get_header(fn, ext=0):
    """Load FITS header from delayed file
    """
    from astropy.io import fits
    from astropy.wcs import WCS
    with copy.copy(fn) as fi:
        with fits.open(fi, memmap=False, cache=False) as hdul:
            hdu = hdul[ext]
            dtype = fits.hdu.image.BITPIX2DTYPE[hdu._bitpix]
            return hdu.header, dtype, hdu.shape, WCS(hdu)
