from .panmethod import PanMethod

class PanParam:
    """PANGAEA Parameter
    Shoud be used to create PANGAEA parameter objects. Parameter is used here to represent 'measured variables'

    Parameters
    ----------
    id : int
        the identifier for the parameter
    name : str
        A long name or title used for the parameter
    shortName : str
        A short name or label to identify the parameter
    param_type : str
        indicates the data type of the parameter (string, numeric, datetime etc..)
    source : str
        defines the category or source for a parameter (e.g. geocode, data, event)... very PANGAEA specific ;)
    unit : str
        the unit of measurement used with this parameter (e.g. m/s, kg etc..)
    terms : list
        a list of dictionaries containing all related terms for this parameter structure:
        example: [{'id': 38477, 'name': 'carbon', 'semantic_uri': 'http://purl.obolibrary.org/obo/CHEBI_27594', 'ontology': 18}, {'id': 44419, 'name': 'organic', 'semantic_uri': None, 'ontology': 18}, {'id': 44029, 'name': 'Molar Flux', 'semantic_uri': None, 'ontology': 13}]

    comment : str
        an optional comment explaining some details of the parameter
    PI : dict
        the responsible PI name of the parameter
    dataseries : int
        the dataseries (column id)
    colno : int
        the column number
    method : PanMethod
        the used method or device



    Attributes
    ----------
    id : int
        the identifier for the parameter
    name : str
        A long name or title used for the parameter
    shortName : str
        A short name or label to identify the parameter
    synonym : dict
        A dictionary of synonyms for the parameter whcih e.g. is used by other archives or communities.
        The dict key indicates the namespace (possible values currently are CF and OS)
    type : str
        indicates the data type of the parameter (string, numeric, datetime etc..)
    source : str
        defines the category or source for a parameter (e.g. geocode, data, event)... very PANGAEA specific ;)
    unit : str
        the unit of measurement used with this parameter (e.g. m/s, kg etc..)
    format: str
        the number format string given by PANGAEA e.g ##.000 which defines the displayed precision of the number
    terms : list
        a list of dictionaries containing all related terms for this parameter
    comment : str
        an optional comment explaining some details of the parameter
    PI : dict
        the responsible PI name of the parameter
    dataseries : int
        the dataseries (column id)
    colno : int
        the column number
    methodid : int
        the id of the used method or device (legacy only)
    method : PanMethod
        the  method (object) used



    """
    def __init__(
        self,
        id: int | None,
        name: str,
        shortName: str,
        param_type: str,
        source: str,
        unit: str | None = None,
        format: str | None = None,
        terms: list[dict[str, int | str | None]] | None = None,
        comment: str | None = None,
        PI: dict[str, str | None] | None = None,
        dataseries: int | None = None,
        colno: str | None = None,
        method: PanMethod | None = None,
    ) -> None:
        self.id = id
        self.name = name
        self.shortName = shortName
        self.type = param_type
        self.source = source
        self.unit = unit
        self.format = format
        self.terms = terms if terms is not None else []
        self.comment = comment
        self.PI = PI
        self.dataseries = dataseries
        self.colno = colno
        self.method = method
        self.methodid = getattr(method, "id", None)
        self.synonym = dict.fromkeys(("CF", "OS", "SD"))

    def addSynonym(
        self,
        ns: str,
        name: str,
        uri: str | None = None,
        id: str | None = None,
        unit: str | None = None,
        unit_id: str | None = None,
    ) -> None:
        self.synonym[ns] = {"name": name, "id": id, "uri": uri, "unit": unit, "unit_id": unit_id}
