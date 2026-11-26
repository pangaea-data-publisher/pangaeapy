class PanMethod:
    """PANGAEA Method Class
    This class creates objects which contain the method context information for e.g. parameters and events

    Parameters
    ----------
    id : str
        The method id internally used in PANGAEA
    name : str
        The full name of the method
    terms: list
        a list of dictionaries containing al related terms for this method
        example : {'id': 38477, 'name': 'carbon', 'semantic_uri': 'http://purl.obolibrary.org/obo/CHEBI_27594', 'ontology': 18}


    Attributes
    ----------
    id : str
        The method id internally used in PANGAEA
    name : str
        The full name of the method
    terms: list
        a list of dictionaries containing al related terms for this method
    """
    def __init__(
        self,
        id: str | None,
        name: str,
        terms: list[dict[str, int | str | None]],
    ) -> None:
        self.id = id
        self.name = name
        self.terms = terms
