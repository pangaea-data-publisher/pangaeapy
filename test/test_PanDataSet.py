#!/usr/bin/env python
"""
| *author*: Johannes Röttenbacher
| *created*: 07.02.2025

Test the PanDataSet class
"""
import io
import os
import pandas as pd
from pangaeapy.pandataset import PanDataSet, PanDataHarvester
from pathlib import Path
import pytest
import re
import zipfile


# needed for the zip download test
@pytest.fixture
def make_fake_zip_bytes(filenames):
    # Create a zip file in memory with the given filenames
    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, 'w') as zf:
        for fname in filenames:
            zf.writestr(fname, "dummy data")
    mem_zip.seek(0)
    return mem_zip.read()


@pytest.fixture
def filenames():
    return [f"filename_{x}.nc" for x in range(11)]


@pytest.fixture
def mock_pandataset(mocker, tmp_path, filenames):
    dataset = mocker.Mock()
    dataset.id = "123456"
    dataset.auth_token = None
    dataset.data = pd.DataFrame({"Binary": filenames})
    dataset.cachedir = tmp_path / "cache"
    # os.makedirs(dataset.cachedir, exist_ok=True)
    dataset.columns = ["Binary"]
    dataset.data_index = []
    # dataset.semaphore = asyncio.Semaphore(5)  # Limit concurrent downloads
    return dataset


def test_default_cache_dir(mocker):
    mock_makedirs = mocker.patch("pathlib.Path.mkdir")
    # mock functionalities, which rely on cache_dir
    mock_connect = mocker.patch("sqlite3.connect")
    mocker.patch.object(PanDataSet, "get_pickle_path")

    ds = PanDataSet(968912, enable_cache=True)
    expected_default = Path(Path.home(), ".pangaeapy_cache")
    assert ds.cachedir == expected_default
    # Ensure PanDataSet tried to create the directory
    mock_makedirs.assert_any_call(parents=True, exist_ok=True)


def test_custom_cache_dir(tmp_path):
    ds = PanDataSet(968912, enable_cache=True, cachedir=tmp_path)
    assert ds.cachedir == tmp_path
    ds.terms_conn.close()  # explicitly close the sqlite database


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
    ds = PanDataSet(944101, enable_cache=True, cachedir=tmp_path)

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


class TestPanDataHarvester:
    """Test the download functionality of pangaeapy"""

    @pytest.mark.parametrize(
        "auth_token, status_code, expected_error",
        [
            (None, 401, "401 Client Error: Unauthorized access."),
            ("valid_token", 200, None)
        ],
        ids=["invalid_token", "valid_token"]
    )
    def test_download_zip_file(
            self, mocker, mock_pandataset, requests_mock, capsys, filenames, auth_token, status_code, expected_error
    ):
        """Test the download of complete binary data sets via the zip download link"""

        class FakeZipFile:
            # fake zip file, which should be downloaded when a valid token is given and self.data_index is []
            def __init__(self, file, mode='r'):
                self.file = file

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

            def namelist(self):
                return filenames

            def extractall(self, path):
                pass  # do nothing

        ds = mock_pandataset
        ds.auth_token = auth_token  # Set token for this test case

        # mock up http response
        matcher = re.compile(r"https://download\.pangaea\.de/dataset/\d+/allfiles\.zip")
        requests_mock.get(matcher, status_code=status_code)

        # patch the ZipFile class
        mocker.patch("zipfile.ZipFile", side_effect=FakeZipFile)

        # Mock os.remove to avoid deleting real files
        mocker.patch("os.remove")

        # initiate the harvester with the mock_pandataset
        harvester = PanDataHarvester(ds)
        result = harvester.run_download()
        # capture console output
        captured = capsys.readouterr()

        # handle the two test cases
        if expected_error:
            assert expected_error in captured.out
            assert result == []
        else:
            # Build expected filepaths
            expected_filepaths = [
                ds.cachedir / f"{fname}" for fname in filenames
            ]
            assert result == expected_filepaths
