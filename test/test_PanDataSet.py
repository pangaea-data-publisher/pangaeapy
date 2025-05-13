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
    (True, ("Binary", "1,3, 2,  5")),
    (True, ("Binary", "1-3, 5")),
    (False, (None, None))
])
def test_netcdf_download(monkeypatch, interactive, test_input):
    # simulate user input only when interactive is True
    if interactive:
        inputs = iter(test_input)
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))

    ds = PanDataSet(944101, enable_cache=True)
    filenames = ds.download(interactive=interactive)

    # check if the user input was parsed correctly
    if any(test_input):
        assert ds.data_index == [1, 2, 3, 5]
        # the data set only has this column so this test is useless
        # keep it and find a better test data set
        assert ds.columns == ["Binary"]

    for filename in filenames:
        assert os.path.isfile(filename)  # check if file was downloaded


@pytest.mark.parametrize(
    "indices, columns, expected_exception",
    [
        ([0], ["Binary"], None),  # Valid case
        ([100], ["Binary"], ValueError),  # Invalid index
        ([0], ["Not working"], ValueError),  # Invalid column
    ],
    ids=["valid_input", "invalid_index", "invalid_column"]
)
def test_download_kwargs(tmp_path, indices, columns, expected_exception):
    """Tests various combinations of download kwargs"""
    ds = PanDataSet(944101, enable_cache=True, cache_dir=tmp_path)

    if expected_exception:
        with pytest.raises(expected_exception):
            ds.download(indices=indices, columns=columns)
    else:
        filenames = ds.download(indices=indices, columns=columns)
        assert all(os.path.isfile(f) for f in filenames), \
            "All expected files should be created"


def test_download_url_handling():
    """Some data sets provide a full download URL in the URL field"""
    ds = PanDataSet(896710, enable_cache=True)
    filename = ds.download(indices=[0])
    assert os.path.isfile(filename[0])