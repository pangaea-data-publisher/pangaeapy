#!/usr/bin/env python
"""
| *author*: Johannes Röttenbacher
| *created*: 07.03.2025

Config functions to use in all test scripts
"""
import pytest


@pytest.fixture(scope="session", autouse=True)
def mock_config_paths(tmp_path_factory):
    # Create a temporary directory for the entire test session
    temp_config_dir = tmp_path_factory.mktemp("mock_config")
    temp_config_path = temp_config_dir / "config.json"

    # Mock the CONFIG_DIR and CONFIG_PATH
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("pangaeapy.pandataset.PanDataSet.CONFIG_DIR", str(temp_config_dir))
        mp.setattr("pangaeapy.pandataset.PanDataSet.CONFIG_PATH", str(temp_config_path))
        yield

