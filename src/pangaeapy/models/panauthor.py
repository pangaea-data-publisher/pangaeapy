class PanAuthor:
    """PANGAEA Author Class.
    A simple helper class to declare 'author' objects which are associated as part of the metadata of a given PANGAEA dataset object

    Parameters
    ----------
    lastname : str
        The author's first name
    firstname : str
        The authors's last name
    ORCID : str
        The unique ORCID identifier assigned by orcid.org
    id : int
        The PANGAEA internal id

    Attributes
    ----------
    lastname : str
        The author's first name
    firstname : str
        The authors's last name
    fullname : str
        Combination of lastname, firstname. This attribute is created by the constructor
    ORCID : str
        The unique ORCID identifier assigned by orcid.org
    id : int
        The PANGAEA internal id
    """
    def __init__(
        self,
        lastname: str,
        firstname: str | None,
        orcid: str | None,
        id: int | None,
        affiliations: list[str],
    ) -> None:
        self.lastname = lastname
        self.firstname = firstname
        self.fullname = f"{lastname}{f', {firstname}' if firstname else ''}"
        self.ORCID = orcid
        self.id = id
        if affiliations:
            self.affiliations = affiliations
