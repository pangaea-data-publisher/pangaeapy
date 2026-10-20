"""
Test the optional labels of events and campaigns, as read from PANGAEA's metadata XML.

An event or campaign can have an optional label (md:optionalLabel; for campaigns, also the
deprecated md:optionalName): another label it is known by, e.g. its station on a cruise, or the
cruise's leg. The tests don't request PANGAEA: test/data holds the metadata of the datasets, as
PANGAEA served it on 2026-10-15.
"""
from pathlib import Path
import xml.etree.ElementTree as ET

from pangaeapy import PanDataSet

DATA = Path(__file__).parent / "data"
NS = {"md": "http://www.pangaea.de/MetaData"}


def load(requests_mock, tmp_path, pan_id, metadata=None):
    """The PanDataSet of `pan_id`, without data, with `metadata` (or that in test/data) served as its metadata XML."""
    if metadata is None:
        metadata = (DATA / f"{pan_id}.xml").read_text(encoding="utf-8")
    url = f"https://doi.pangaea.de/10.1594/PANGAEA.{pan_id}"
    requests_mock.get(url, request_headers={"Accept": "application/vnd.pangaea.metadata+xml"}, text=metadata)
    requests_mock.get(url, request_headers={"Accept": "text/x-bibliography"}, text="A citation")
    ds = PanDataSet(pan_id, include_data=False, cachedir=tmp_path)
    ds.terms_conn.close()
    return ds


def test_optional_labels_of_events(requests_mock, tmp_path):
    """Events have their optional label, their station on cruise M74/2; their campaign has none."""
    ds = load(requests_mock, tmp_path, 828672)
    assert {label: event.optionalLabel for label, event in ds.events.items()} == {
        "GeoB12204-4": "M74/2_969-1",
        "GeoB12208-2": "M74/2_976-2",
        "GeoB12210-3": "M74/2_979-3",
        "GeoB12216-5": "M74/2_986-5",
    }
    assert {event.campaign.optionalLabel for event in ds.events.values()} == {None}


def test_optional_label_of_campaign(requests_mock, tmp_path):
    """Campaign PS113 is also known as ANT-XXXIII/4; its event has no optional label."""
    event = load(requests_mock, tmp_path, 996416).events["PS113-track"]
    assert event.optionalLabel is None
    assert event.campaign.optionalLabel == "ANT-XXXIII/4"


def test_optional_name_of_campaign(requests_mock, tmp_path):
    """A campaign with only the deprecated md:optionalName has that as its optional label."""
    ET.register_namespace("md", NS["md"])
    root = ET.fromstring((DATA / "996416.xml").read_text(encoding="utf-8"))
    for campaign in root.iterfind("md:event/md:campaign", NS):
        campaign.remove(campaign.find("md:optionalLabel", NS))
    ds = load(requests_mock, tmp_path, 996416, ET.tostring(root, encoding="unicode"))
    assert ds.events["PS113-track"].campaign.optionalLabel == "ANT-XXXIII/4"
