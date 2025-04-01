#!/usr/bin/env python
"""
Test the PanQuery class
"""

from pangaeapy import PanQuery

def test_get_doi():
    """Test the correct retrieval of DOIs from the search result."""
    dois = ["doi:10.1594/PANGAEA.956151", "doi:10.1594/PANGAEA.956156"]
    query = " or ".join(dois)
    result = PanQuery(query)
    doi_retrieved = result.get_doi()
    assert all([doi in doi_retrieved for doi in dois])
