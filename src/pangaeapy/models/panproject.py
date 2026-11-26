class PanProject:
    """PANGAEA Project Class
    This class creates objects which contain the project context information for each dataset

    Parameters
    ----------
    acronym : str
        The project acronym
    title : str
        The full title of the project
    URL : str
        The project website
    awardURI : str
        The unique identifier of the award e.g. a CORDIS URI
    id : int
        internal project id



    Attributes
    ----------
    acronym : str
        The project acronym
    title : str
        The full title of the project
    URL : str
        The project website
    awardURI : str
        The unique identifier of the award e.g. a CORDIS URI
    id : int
        internal project id

    """
    def __init__(
        self,
        label: str | None,
        name: str | None,
        URL: str | None,
        awardURI: str | None,
        id: int | None,
    ) -> None:
        self.label = label
        self.name = name
        self.URL = URL
        self.awardURI = awardURI
        self.id = id
