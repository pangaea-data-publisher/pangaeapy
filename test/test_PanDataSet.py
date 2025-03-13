#!/usr/bin/env python
"""
| *author*: Johannes Röttenbacher
| *created*: 07.02.2025

Test the PanDataSet class
"""
import os
from pangaeapy.pandataset import PanDataSet
import xarray as xr

def test_default_cache_dir():
    ds = PanDataSet(968912, enable_cache=True)
    assert ds.cache_dir is not None
    assert os.path.isdir(ds.cache_dir)  # Ensure the directory was created


def test_custom_cache_dir(tmp_path):
    ds = PanDataSet(968912, enable_cache=True, cache_dir=tmp_path)
    assert ds.cache_dir == tmp_path
    ds.terms_conn.close()  # explicitly close the sqlite database

def test_netcdf_download(monkeypatch):
    # Simulate user input
    monkeypatch.setattr('builtins.input', lambda _: '1,2,4')
    ds = PanDataSet(944101, enable_cache=True)
    filenames = ds.download()
    for filename in filenames:
        assert os.path.isfile(filename)  # check if file was downloaded
