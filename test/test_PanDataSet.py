#!/usr/bin/env python
"""
| *author*: Johannes Röttenbacher
| *created*: 07.02.2025

Test the PanDataSet class
"""
import os
from pangaeapy.pandataset import PanDataSet
import pytest


def test_default_cache_dir():
    ds = PanDataSet(968912, enable_cache=True)
    assert ds.cache_dir is not None
    assert os.path.isdir(ds.cache_dir)  # Ensure the directory was created


def test_custom_cache_dir(tmp_path):
    ds = PanDataSet(968912, enable_cache=True, cache_dir=tmp_path)
    assert ds.cache_dir == tmp_path
    ds.terms_conn.close()  # explicitly close the sqlite database


@pytest.mark.parametrize("interactive, test_input", [
    (True, ("", "")),
    (True, ("Binary", "1,3, 4,  5")),
    (False, None)
])
def test_netcdf_download(monkeypatch, interactive, test_input):
    # simulate user input only when interactive is True
    if interactive:
        inputs = iter(test_input)
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))

    ds = PanDataSet(944101, enable_cache=True)
    filenames = ds.download(interactive=interactive)

    for filename in filenames:
        assert os.path.isfile(filename)  # check if file was downloaded
