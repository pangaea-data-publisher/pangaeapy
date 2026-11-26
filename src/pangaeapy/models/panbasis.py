class PanBasis:
    """PANGAEA Basis class
    The basis or platform which was used during the event e.g. a ship

    name : str
        The name of the platform
    URI : str
        The platform URI
    callSign : str
        A unique identifier of the platform(alphabet)
    IMOnumber : str
        A seven digit unique identifier for the platform
    """
    def __init__(
        self,
        name: str,
        URI: str | None,
        callSign: str | None,
        IMOnumber: str | None,
    ) -> None:
        self.name = name
        self.URI = URI
        self.callSign = callSign
        self.IMOnumber = IMOnumber
