class PanLicence:
    """PANGAEA Licence Class
    This class contains the licence information for each dataset
    """
    def __init__(
        self,
        label: str | None,
        name: str | None,
        URI: str | None,
    ) -> None:
        self.label = label
        self.name = name
        self.URI = URI
