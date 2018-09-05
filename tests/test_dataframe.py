from astropy.table import Table
import intake
import os
import pandas as pd
import numpy as np


df = pd.DataFrame({'a': np.random.randint(0, 123412, size=1000),
                   'b': np.random.randn(1000),
                   'c': np.random.choice([True, False], size=1000),
                   'd': np.random.randint(0, 123, size=1000, dtype='u2')
                   }
                  )


def test_simple(tmpdir):
    t = Table.from_pandas(df)
    fn = os.path.join(tmpdir, 'one.fits')
    t.write(fn, format='fits')
    s = intake.open_fits_table(fn, ext=1)
    out = s.read()
    assert (out.reset_index(drop=True) ==
            df.reset_index(drop=True)).all().all()


def test_multiple(tmpdir):
    t = Table.from_pandas(df)
    for name in ['one', 'two', 'three']:
        fn = os.path.join(tmpdir, '%s.fits' % name)
        t.write(fn, format='fits')
    fn = os.path.join(tmpdir, '*.fits')
    s = intake.open_fits_table(fn, ext=1)
    out = s.read()
    assert (out.reset_index(drop=True) ==
            pd.concat([df, df, df]).reset_index(drop=True)).all().all()


def test_section(tmpdir):
    t = Table.from_pandas(df)
    for name in ['one', 'two', 'three']:
        fn = os.path.join(tmpdir, '%s.fits' % name)
        t.write(fn, format='fits')
    fn = os.path.join(tmpdir, '*.fits')
    s = intake.open_fits_table(fn, chunksize=300, ext=1)
    d = s.to_dask()
    assert d.npartitions == 12
    out = s.read()
    assert (out.reset_index(drop=True) ==
            pd.concat([df, df, df]).reset_index(drop=True)).all().all()
