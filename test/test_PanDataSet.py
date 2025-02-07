#!/usr/bin/env python
"""
| *author*: Johannes Röttenbacher
| *created*: 07.02.2025

Test the PanDataSet class
"""
import os
import tempfile
from pangaeapy.pandataset import PanDataSet

def test_default_cache_dir():
    ds = PanDataSet(968912, enable_cache=True)
    assert ds.cachedir is not None
    assert os.path.isdir(ds.cachedir)  # Ensure the directory was created


def test_custom_cache_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        ds = PanDataSet(968912, enable_cache=True, cache_dir=tmpdir)
        assert ds.cachedir == tmpdir
        ds.terms_conn.close()  # explicitly close the sqlite database

