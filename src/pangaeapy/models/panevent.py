from .panbasis import PanBasis
from .pancampaign import PanCampaign
from .panmethod import PanMethod

class PanEvent:
    """PANGAEA Event Class.
    An Event can be regarded as a named entity which is defined by the usage of a distinct method or device at a distinct location during a given time interval for scientific purposes.
    More infos on PANGAEA's Evenmts can be found here: https://wiki.pangaea.de/wiki/Event

    Parameters
    ----------
    label : str
        A label which is used to name the event
    latitude : float
        The latitude of the event location
    longitude : float
        The longitude of the event location
    elevation : float
        The elevation (relative to sea level) of the event location
    datetime : str
        The date and time of the event in ´%Y/%m/%dT%H:%M:%S´ format
    device : str
        The device which was used during the event

    Attributes
    ----------
    label : str
        A label which is used to name the event
    latitude : float
        The start latitude of the event location
    longitude : float
        The start longitude of the event location
    latitude2 : float
        The end latitude of the event location
    longitude2 : float
        The end longitude of the event location
    elevation : float
        The elevation (relative to sea level) of the event location
    datetime : str
        The start date and time of the event in ´%Y/%m/%dT%H:%M:%S´ format
    datetime2 : str
        The end date and time of the event in ´%Y/%m/%dT%H:%M:%S´ format
    device : str
        The device which was used during the event
    basis : PanBasis
        The basis or platform which was used during the event e.g. a ship
    location : str
        The location of the event
    campaign : PanCampaign
        The campaign during which the event was performed
    optionalLabel : str
        Another label the event is known by, e.g. its station on the campaign (md:optionalLabel)
    """
    def __init__(
        self,
        label: str,
        latitude: str | None,
        longitude: str | None,
        latitude2: str | None,
        longitude2: str | None,
        elevation: str | None,
        datetime: str | None,
        datetime2: str | None,
        basis: PanBasis | None,
        location: str | None,
        campaign: PanCampaign | None,
        id: str | None,
        method: PanMethod | None,
        optionalLabel: str | None,
    ) -> None:
        self.label = label
        self.latitude = float(latitude) if latitude is not None else None
        self.longitude = float(longitude) if longitude is not None else None
        self.latitude2 = float(latitude2) if latitude2 is not None else None
        self.longitude2 = float(longitude2) if longitude2 is not None else None
        self.elevation = float(elevation) if elevation is not None else None
        self.datetime = datetime
        self.datetime2 = datetime2
        self.basis = basis
        self.location = location
        self.campaign = campaign
        self.id = id
        self.method = method
        self.deviceid = getattr(method, "id", None)
        self.device = getattr(method, "name", None)
        self.optionalLabel = optionalLabel
