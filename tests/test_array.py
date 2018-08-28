from astropy.io import fits
from astropy.wcs import WCS
import intake
import os
import pytest

here = os.path.dirname(os.path.abspath(__file__))
fn = os.path.join(here, 'WFPC.fits')
fn2 = os.path.join(here, 'WFPC_copy.fits')
data = fits.getdata(fn)


def test_single():
    s = intake.open_fits_array(fn)
    sch = s.discover()
    assert sch['dtype'] == 'float32'
    assert sch['shape'] == data.shape
    assert sch['npartitions'] == 1
    assert isinstance(sch['metadata'], dict)
    assert sch['metadata']['SIMPLE'] is True
    assert isinstance(s.wcs, WCS)
    assert (s.read() == data).all()


@pytest.mark.parametrize("f", [[fn, fn2],
                               os.path.join(here, 'WFPC*.fits')])
def test_multiple(f):
    s = intake.open_fits_array(f)
    out1 = s.read_partition(0)
    out2 = s.read_partition(1)
    assert (out1 == data).all()
    assert (out2 == data).all()
    out = s.read()
    assert (out[0] == data).all()
    outd = s.to_dask()
    assert outd.npartitions == 2
    assert outd.shape == (2, ) + data.shape


def test_slice():
    s = intake.open_fits_array(fn, chunks=(2, 200, 200))
    out = s.to_dask()
    assert out.npartitions == 2
    assert (s.read_partition((0, 0, 0)) == data[:2, :, :]).all()
    assert (s.read() == data).all()
