class PanCampaign:
    """PANGAEA Campaign class
    The campaign during which the event took place, e.g. a ship cruise or observatory deployment

    name : str
        The name of the campaign (md:label; md:name in older records)
    URI : str
        The campaign URI
    start : str
        The start date of the campaign (md:dateTime, e.g. 2018-05-08T00:00:00; md:start, e.g.
        2018-05-08, in older records)
    end : str
        The end date of the campaign (md:dateTime2; md:end in older records)
    startlocation : str
        The start location of the campaign, e.g. a port (md:location; the attribute
        "Start location" in older records)
    endlocation : str
        The end location of the campaign (md:location2; the attribute "End location" in older
        records)
    BSHID : str
        The BSH ID for the campaign
    expeditionprogram: str
        URL of the campaign report
    optionalLabel : str
        Another label the campaign is known by, e.g. ANT-XXXIII/4 for PS113 (md:optionalLabel;
        md:optionalName in older records)
    """
    def __init__(
        self,
        name: str,
        URI: str | None,
        start: str | None,
        end: str | None,
        startlocation: str | None,
        endlocation: str | None,
        BSHID: str | None,
        expeditionprogram: str | None,
        optionalLabel: str | None,
    ) -> None:
        self.name = name
        self.URI = URI
        self.start = start
        self.end = end
        self.startlocation = startlocation
        self.endlocation = endlocation
        self.BSHID = BSHID
        self.expeditionprogram = expeditionprogram
        self.optionalLabel = optionalLabel
