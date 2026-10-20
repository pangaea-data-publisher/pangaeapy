#!/usr/bin/env python
"""
Test the campaigns of events, as read from PANGAEA's metadata XML.

PANGAEA's metadata names a campaign by md:label, with md:dateTime, md:dateTime2, md:location and
md:location2. The former elements md:name, md:start and md:end (and the attributes "Start location"
and "End location") are deprecated: records of format 2026-04-13 have both forms, records of
format 2026-08-17 only the current one. The tests don't request PANGAEA: test/data holds the
metadata of the datasets, as PANGAEA served it on 2026-10-15.
"""
from pathlib import Path
import xml.etree.ElementTree as ET

import pytest

from pangaeapy import PanDataSet

DATA = Path(__file__).parent / "data"
NS = {"md": "http://www.pangaea.de/MetaData"}


def load(requests_mock, tmp_path, pan_id, metadata):
    """The PanDataSet of `pan_id`, without data, with `metadata` served as its metadata XML."""
    url = f"https://doi.pangaea.de/10.1594/PANGAEA.{pan_id}"
    requests_mock.get(url, request_headers={"Accept": "application/vnd.pangaea.metadata+xml"}, text=metadata)
    requests_mock.get(url, request_headers={"Accept": "text/x-bibliography"}, text="A citation")
    ds = PanDataSet(pan_id, include_data=False, cachedir=tmp_path)
    ds.terms_conn.close()
    return ds


def deprecated_only(metadata):
    """`metadata` with the campaigns in the deprecated form only, as in records before 2026-04-13."""
    ET.register_namespace("md", NS["md"])
    root = ET.fromstring(metadata)
    for campaign in root.iterfind("md:event/md:campaign", NS):
        for path in ("md:label", "md:dateTime", "md:dateTime2"):
            campaign.remove(campaign.find(path, NS))
        for path, name in (("md:location", "Start location"), ("md:location2", "End location")):
            location = campaign.find(path, NS)
            campaign.remove(location)
            ET.SubElement(campaign, f"{{{NS['md']}}}attribute", name=name).text = location.find("md:name", NS).text
    return ET.tostring(root, encoding="unicode")


@pytest.mark.parametrize(
    "pan_id, deprecated, campaign",
    [
        # format 2026-08-17: only the current elements
        (828672, False, ("M74/2", "2007-10-07T00:00:00", "2007-10-28T00:00:00",
                         "Fujayrah Harbor, United Arab Emirates", "Fujayrah Harbor, United Arab Emirates", "20080085")),
        # format 2026-04-13: both forms, the current elements are read
        (996416, False, ("PS113", "2018-05-08T00:00:00", "2018-06-11T00:00:00",
                         "Punta Arenas, Chile", "Bremerhaven, Germany", "20180003")),
        # only the deprecated elements and attributes
        (996416, True, ("PS113", "2018-05-08", "2018-06-11",
                        "Punta Arenas, Chile", "Bremerhaven, Germany", "20180003")),
    ],
    ids=["current", "both", "deprecated"],
)
def test_campaign(requests_mock, tmp_path, pan_id, deprecated, campaign):
    """Every event's campaign has its name, dates and ports, in either form of the metadata."""
    metadata = (DATA / f"{pan_id}.xml").read_text(encoding="utf-8")
    if deprecated:
        metadata = deprecated_only(metadata)
    ds = load(requests_mock, tmp_path, pan_id, metadata)
    assert ds.events
    for event in ds.events.values():
        c = event.campaign
        assert (c.name, c.start, c.end, c.startlocation, c.endlocation, c.BSHID) == campaign
