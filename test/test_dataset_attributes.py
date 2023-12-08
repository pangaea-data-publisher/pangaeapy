"""
Created on Fri Dec 8 11:40:27 2023

@author: Florian Spreckelsen
"""

from pangaeapy import PanDataSet


def test_keywords():
    """Simple snapshot test of one example real-world dataset,
    https://doi.pangaea.de/10.1594/PANGAEA.957810. Note that this test may fail
    if at some point in the future, the keywords of this example set change on
    PANGAEA.

    """

    ds = PanDataSet("https://doi.pangaea.de/10.1594/PANGAEA.957810", include_data=False)
    assert hasattr(ds, "keywords")
    assert isinstance(ds.keywords, list)

    # Update these in case of changes on PANGAEA
    remote_keywords = [
        "Benguela Upwelling System",
        "elemental stoichiometry",
        "marine carbon cycle",
        "Sea surface partial pressure of CO2"
    ]
    assert len(ds.keywords) == len(remote_keywords)
    for rkw in remote_keywords:
        assert rkw in ds.keywords
