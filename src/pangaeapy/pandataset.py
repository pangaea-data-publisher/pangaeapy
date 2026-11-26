from io import StringIO
import json
import logging
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests
from requests.exceptions import RequestException

from .exporter import PanDarwinCoreArchiveExporter, PanFrictionlessExporter, PanNetCDFExporter
from .models import PanAuthor, PanBasis, PanCampaign, PanEvent, PanLicence, PanMethod, PanParam, PanProject
from .pandataharvester import PanDataHarvester
from .utils import get_id, load_data, load_metadata, load_terms

logger = logging.getLogger(__name__)

class PanDataSet:
    """PANGAEA DataSet
    The PANGAEA PanDataSet class enables the creation of objects which hold the necessary information, including data as well as metadata, to analyse a given PANGAEA dataset.

    Parameters
    ----------
    id : str
        The identifier of a PANGAEA dataset. An integer number or a DOI is accepted here
    deleteFlag : str
        in case quality flags are avialable, this parameter defines a flag for which data should not be included in the data dataFrame.
        Possible values are listed here: https://wiki.pangaea.de/wiki/Quality_flag
    enable_cache : boolean
        If set to True, PanDataSet objects are cached as pickle files either on the local home directory within a directory called '.pangaeapy_cache' or in cachedir given by the user in order to avoid unnecessary downloads.
    include_data : boolean
        determines if data table is downloaded and added to the self.data dataframe. If you are interested in metadata only set this to False
    expand_terms : list or int
        indicates if found ontology terms for parameters shall be expanded for the given list of terminology ids, i.p. add their hierarchy terms / classification

    Attributes
    ----------
    id : int
        The identifier of a PANGAEA dataset. An integer number or a DOI is accepted here
    uri : str
        The PANGAEA DOI (alternative label)
    doi : str
        The PANGAEA DOI
    title : str
        The title of the dataset
    abstract: str
        the abstract or summary of the dataset
    year : int
        The publication year of the dataset
    authors : list of PanAuthor
        a list containing the PanAuthot objects (author info) of the dataset
    citation : str
        the full citation of the dataset including e.g. author, year, title etc..
    params : list of PanParam
        a list of all PanParam objects (the parameters) used in this dataset
    parameters : list of PanParam
        synonym for self.params
    events : list of PanEvent
        a list of all PanEvent objects (the events) used in this dataset
    projects : list of PanProject
        a list containing the PanProjects objects referenced by this dataset
    mintimeextent : str
        a string containing the min time of data set extent
    maxtimeextent : str
        a string containing the max time of data set extent
    data : pandas.DataFrame
        a pandas dataframe holding all the data
    loginstatus : str
        a label which indicates if the data set is protected or not default value: 'unrestricted'
    isCollection : boolean
        indicates if this dataset is a collection data set within a collection of child data sets
    collection_members : list
        a list of DOIs of all child data sets in case the data set is a collection data set
    moratorium : str
        a label which provides the date until the dataset is under moratorium
    datastatus : str
        a label which provides the detail about the status of the dataset whether it is published or in review or deleted
    registrystatus : str
        a string which indicates the registration status of a dataset
    licence : PanLicence
        a licence object, usually creative commons
    auth_token : str
        the PANGAEA auhentication token, you can find it at https://www.pangaea.de/user/
    cache_expiry_days : int
        the duration a cached pickle/cache is accepted, after this pangaeapy will load it again and ignor ethe cache
    cachedir: str
        the full path to the cache directory, will be created if it doesn't exist
    keywords : list[str]
        A list of keyword names. Only actual keywords, technical and
        auto-generated ones are ignored right now.

    """
    def __init__(
        self,
        id: int | str | None = None,
        paramlist: list[str] | None = None,
        deleteFlag: str | None = None,
        enable_cache: bool = False,
        cachedir: str | os.PathLike = Path(Path.home(), ".pangaeapy_cache"),
        include_data: bool = True,
        expand_terms: int | list[int] | None = None,
        auth_token: str | None = None,
        cache_expiry_days: int = 1,
    ) -> None:
        self.logging = []
        self.logger = logger
        try:
            self.id = int(str(id).split(".")[-1])
        except ValueError:
            self.log(logging.ERROR, f"Invalid identifier or DOI: {id}")
            self.id = None
        self.cache = enable_cache
        self.cachedir = Path(cachedir)
        self.cache_expiry_days = int(max(cache_expiry_days, 1))
        if self.id is not None and enable_cache:
            try:
                cachedir.mkdir(parents=True, exist_ok=True)
            except OSError:
                self.log(logging.WARNING, "Unable to initialize cache")
                self.cache = False
        self.expand_terms = [expand_terms] if isinstance(expand_terms, int) else expand_terms if isinstance(expand_terms, list) else []
        self.include_data = include_data
        self.auth_token = auth_token
        self.paramlist = paramlist
        self.deleteFlag = deleteFlag if deleteFlag is not None else ""
        self.setMetadata()
        self.defaultparams = ["Latitude", "Longitude", "Event", "Elevation", "Date/Time"]
        self.paramlist_index = []
        self.data = pd.DataFrame()
        self.qcdata = pd.DataFrame()
        self.quality_flags = {"ok": "valid", "?": "questionable", "/": "not_valid", "*": "unknown"}
        self.quality_flag_replace = {"ok": 0, "?": 1, "/": 2, "*": 3}
        if self.id is not None:
            if (self.loginstatus == "unrestricted" or self.auth_token) and not self.isCollection:
                self.setData()
                self.defaultparams = [s for s in self.defaultparams if s in self.params.keys()]
                if self.paramlist is not None:
                    if len(self.paramlist) != len(self.paramlist_index):
                        self.log(logging.WARNING, f"Inconsistent number of detected parameters, expected: {len(self.paramlist)} vs {len(self.paramlist_index)}")
            else:
                self.log(logging.WARNING, "Dataset is either restricted or of type 'collection'")

    def log(self, level: int, message: str) -> None:
        message = f"{message} - {getattr(self, 'doi', '')}"
        self.logging.append({logging.getLevelName(level): message})
        self.logger.log(level, message)

    def find(self, match: str, root: ET.Element | None = None) -> ET.Element | None:
        return (self._xml_root if root is None else root).find(match, namespaces=self.ns) if self._xml_root is not None else None

    def findall(self, match: str, root: ET.Element | None = None) -> list[ET.Element]:
        return (self._xml_root if root is None else root).findall(match, namespaces=self.ns) if self._xml_root is not None else []

    def findtext(self, match: str, default: str | None = None, root: ET.Element | None = None) -> str | None:
        return (self._xml_root if root is None else root).findtext(match, default=default, namespaces=self.ns) if self._xml_root is not None else default

    def findattrib(self, match: str, key: str, default: str | None = None, root: ET.Element | None = None) -> str | None:
        return node.get(key, default=default) if (node := self.find(match, root)) is not None else default

    def setMetadata(self) -> None:
        self.metaxml = None
        self.citation = None
        if self.id is not None:
            try:
                self.metaxml, self.citation = load_metadata(
                    self.id,
                    self.cache,
                    self.cachedir,
                    self.cache_expiry_days,
                )
            except IndexError:
                self.log(logging.ERROR, f"Dataset does not exist: {self.id}")
                self.id = None
            except RequestException as exc:
                self.log(logging.ERROR, f"An error occurred while trying to load metadata: {exc}")
        self._xml_root = ET.fromstring(self.metaxml) if self.metaxml is not None else None
        self.ns = {"md": "http://www.pangaea.de/MetaData"}
        self.abstract = self.findtext("./md:abstract")
        self.allowNetCDF = True
        self.authors = [
            PanAuthor(
                self.findtext("./md:lastName", root=author),
                self.findtext("./md:firstName", root=author),
                self.findtext("./md:orcid", root=author),
                get_id(author, cast=True),
                [
                    get_id(affiliation)
                    for affiliation in self.findall("./md:affiliation", root=author)
                ],
            )
            for author in self.findall("./md:citation/md:author")
        ]
        self.collection_members = [
            f"doi:10.1594/PANGAEA.{child[1:]}"
            for child in self.findattrib("./md:technicalInfo/md:entry[@key='collectionChilds']", "value", default="").split(",")
            if child
        ]
        self.comment = self.findtext("./md:comment")
        self.curationlevel = self.findtext("./md:status/md:curationLevel/md:name")
        self.datastatus = self.findattrib("./md:technicalInfo/md:entry[@key='status']", "value")
        self.date = self.findtext("./md:citation/md:dateTime")
        self.doi = self.uri = self.findtext("./md:citation/md:URI", default="")
        self.eventInMatrix = any(short_name.text == "Event" for short_name in self.findall("./md:matrixColumn/md:parameter/md:shortName"))
        self.events = {
            self.findtext("./md:label", root=event): PanEvent(
                self.findtext("./md:label", root=event),
                self.findtext("./md:latitude", root=event),
                self.findtext("./md:longitude", root=event),
                self.findtext("./md:latitude2", root=event),
                self.findtext("./md:longitude2", root=event),
                self.findtext("./md:elevation", root=event),
                self.findtext("./md:dateTime", root=event),
                self.findtext("./md:dateTime2", root=event),
                PanBasis(
                    self.findtext("./md:name", root=basis),
                    self.findtext("./md:URI", root=basis),
                    self.findtext("./md:callSign", root=basis),
                    self.findtext("./md:IMOnumber", root=basis),
                ) if (basis := self.find("./md:basis", root=event)) is not None else None,
                self.findtext("./md:location/md:name", root=event),
                PanCampaign(
                    self.findtext("./md:label", root=campaign),
                    self.findtext("./md:URI", root=campaign),
                    self.findtext("./md:dateTime", root=campaign),
                    self.findtext("./md:dateTime2", root=campaign),
                    self.findtext("./md:location/md:name", root=campaign),
                    self.findtext("./md:location2/md:name", root=campaign),
                    self.findtext("./md:attribute[@name='BSH ID']", root=campaign),
                    self.findtext("./md:attribute[@name='Expedition Program']", root=campaign),
                    self.findtext("./md:optionalLabel", root=campaign),
                ) if (campaign := self.find("./md:campaign", root=event)) is not None else None,
                get_id(event),
                PanMethod(
                    get_id(method),
                    self.findtext("./md:name", root=method),
                    [
                        {
                            "id": get_id(term, cast=True),
                            "name": self.findtext("./md:name", root=term),
                            "semantic_uri": term.get("semanticURI"),
                            "ontology": int(term.get("terminologyId")),
                        }
                        for term in self.findall("./md:term", root=method)
                    ],
                ) if (method := self.find("./md:method", root=event)) is not None else None,
                self.findtext("./md:optionalLabel", root=event),
            )
            for event in self.findall("./md:event")
        }
        self.geometryextent = {
            key: val
            for key in (
                "westBoundLongitude",
                "eastBoundLongitude",
                "southBoundLatitude",
                "northBoundLatitude",
                "meanLongitude",
                "meanLatitude",
            )
            if (val := self.findtext(f"./md:extent/md:geographic/md:{key}")) is not None
        }
        self.isCollection = self.find("./md:technicalInfo/md:entry[@key='collectionType']") is not None
        self.keywords = [keyword.text for keyword in self.findall("./md:keywords/md:keyword")]
        self.lastupdate = self.findattrib("./md:technicalInfo/md:entry[@key='lastModified']", "value", default=self.date)
        self.licence = PanLicence(
            self.findtext("./md:label", root=licence),
            self.findtext("./md:name", root=licence),
            self.findtext("./md:URI", root=licence),
        ) if (licence := self.find("./md:license")) is not None else None
        self.loginstatus = self.findattrib("./md:technicalInfo/md:entry[@key='loginOption']", "value", default="unrestricted")
        self.maxtimeextent = self.findtext("./md:extent/md:temporal/md:maxDateTime")
        self.mintimeextent = self.findtext("./md:extent/md:temporal/md:minDateTime")
        self.moratorium = self.findattrib("./md:technicalInfo/md:entry[@key='moratoriumUntil']", "value")
        self.params = self.parameters = {
            f"{self.findtext('./md:parameter/md:shortName', root=matrix_column)}_{count}"
            if (count := list(map(
                lambda el: el.text, self.findall("./md:matrixColumn/md:parameter/md:shortName")
            ))[:i+1].count(self.findtext("./md:parameter/md:shortName", root=matrix_column))) > 1
            else self.findtext("./md:parameter/md:shortName", root=matrix_column): PanParam(
                get_id(self.find("./md:parameter", root=matrix_column), cast=True),
                self.findtext("./md:parameter/md:name", root=matrix_column),
                self.findtext("./md:parameter/md:shortName", root=matrix_column),
                matrix_column.get("type"),
                matrix_column.get("source"),
                self.findtext("./md:parameter/md:unit", root=matrix_column),
                matrix_column.get("format"),
                [{
                    "id": get_id(term, cast=True),
                    "name": self.findtext("./md:name", root=term),
                    "semantic_uri": term.get("semanticURI"),
                    "ontology": int(term.get("terminologyId")),
                } for term in self.findall("./md:parameter/md:term", root=matrix_column)],
                self.findtext("./md:comment", root=matrix_column),
                {
                    "id": get_id(pi),
                    "name": self.findtext("./md:lastName", root=pi) + f", {first_name}" if (first_name := self.findtext("./md:firstName", root=pi)) is not None else "",
                    "last_name": self.findtext("./md:lastName", root=pi),
                    "first_name": self.findtext("./md:firstName", root=pi),
                } if (pi := self.find("./md:PI", root=matrix_column)) is not None else None,
                get_id(matrix_column, cast=True) if self.findattrib("./md:parameter", "id", root=matrix_column) is not None else None,
                matrix_column.get("col"),
                PanMethod(
                    get_id(method),
                    self.findtext("./md:name", root=method),
                    [
                        {
                            "id": get_id(term, cast=True),
                            "name": self.findtext("./md:name", root=term),
                            "semantic_uri": term.get("semanticURI"),
                            "ontology": int(term.get("terminologyId")),
                        }
                        for term in self.findall("./md:term", root=method)
                    ],
                ) if (method := self.find("./md:method", root=matrix_column)) is not None else None,
            )
            for i, matrix_column in enumerate(self.findall("./md:matrixColumn"))
        }
        self.processinglevel = self.findtext("./md:status/md:processingLevel/md:name")
        self.projects = [
            PanProject(
                self.findtext("./md:label", root=project),
                self.findtext("./md:name", root=project),
                self.findtext("./md:URI", root=project),
                self.findtext("./md:award/md:URI", root=project),
                get_id(project, cast=True),
            )
            for project in self.findall("./md:project")
        ]
        self.registrystatus = self.findattrib("./md:technicalInfo/md:entry[@key='DOIRegistryStatus']", "value")
        self.relations = [
            {
                "id": reference.get("id"),
                "title": self.findtext("./md:title", root=reference),
                "uri": self.findtext("./md:URI", root=reference),
                "type": reference.get("relationType"),
            }
            for reference in self.findall("./md:reference")
        ]
        self.supplement_to = {
            "id": supplement_to.get("id"),
            "title": self.findtext("./md:title", root=supplement_to),
            "uri": self.findtext("./md:URI", root=supplement_to),
            "year": self.findtext("./md:year", root=supplement_to),
        } if (supplement_to := self.find("./md:citation/md:supplementTo")) is not None else {}
        self.title = self.findtext("./md:citation/md:title")
        self.topotype = self.findtext("./md:extent/md:topoType")
        self.year = self.findtext("./md:citation/md:year")
        if self.id is not None:
            if self.datastatus is None or self.datastatus in {"deleted", "draft"}:
                self.log(logging.ERROR, f"Dataset is deleted, a draft or of unknown status: {self.datastatus}")
            if self.loginstatus != "unrestricted" and not self.auth_token:
                self.log(logging.WARNING, "Dataset is protected")
            if self.isCollection:
                self.log(logging.WARNING, "Dataset is of type collection, please select one of its child datasets")
        if self.datastatus is not None and self.datastatus not in {"deleted", "draft"} and self.expand_terms:
            try:
                terms_info = {
                    int(term): [
                        topic
                        for topics in topics_dict.values()
                        for topic in ([topics] if isinstance(topics, str) else topics)
                    ]
                    for term, topics_dict in load_terms(
                        sorted({
                            get_id(term)
                            for term in self.findall(".//md:term")
                            if int(term.get("terminologyId")) in self.expand_terms
                        }),
                        self.cache,
                        self.cachedir,
                        self.cache_expiry_days,
                    ).items()
                }
            except RequestException:
                self.log(logging.WARNING, "Failed to load extended term info")
            else:
                for param in self.params.values():
                    for term in getattr(param.method, "terms", []):
                        term["classification"] = terms_info.get(term["id"], [])
                    for term in param.terms:
                        term["classification"] = terms_info.get(term["id"], [])
                for event in self.events.values():
                    for term in getattr(event.method, "terms", []):
                        term["classification"] = terms_info.get(term["id"], [])

    def getEventsAsFrame(self) -> pd.DataFrame:
        """
        For more convenient handling of event info, this method returns a dataframe containing all events with their attributes as columns
        Please note that this version just takes campaign names, not other campaign attributes
        """
        df = pd.DataFrame()
        try:
            df = pd.DataFrame([ev.__dict__ for ev in self.events.values()])
            df["campaign"] = df["campaign"].apply(lambda x: x.name)
        except:
            pass
        return df

    def setData(self, addEventColumns: bool = True) -> None:
        """
        This method populates the data DataFrame with data from a PANGAEA dataset.
        In addition to the data given in the tabular ASCII file delivered by PANGAEA.

        Parameters
        ----------
        addEventColumns : boolean
            In case Latitude, Longitude, Elevation, Date/Time and Event are not given in the ASCII matrix, which sometimes is possible in single Event datasets,
            the setData could add these columns to the dataframe using the information given in the metadata for Event. Default is 'True'

        """
        if self.paramlist is not None:
            self.paramlist += self.defaultparams
            for parameter in self.paramlist:
                iter = 0
                for shortName in self.params.keys():
                    if parameter == shortName:
                        self.paramlist_index.append(iter)
                    iter += 1
            if len(self.paramlist) != len(self.paramlist_index):
                self.log(logging.WARNING, "Error entering parameters`short names, inconsitent number of parameters")
        else:
            self.paramlist_index = None
        if self.include_data:
            try:
                panData = load_data(self.id, self.cache, self.cachedir, self.cache_expiry_days, self.auth_token)
                self.data = pd.read_csv(StringIO(panData), index_col=False, on_bad_lines="skip", sep="\t", usecols=self.paramlist_index, names=list(self.params.keys()), skiprows=[0], low_memory=False)
                if addEventColumns:
                    if len(self.events.items()) == 1:
                        if "Event" not in self.data.columns:
                            self.data["Event"] = list(self.events.values())[0].label
                            self.params["Event"] = PanParam(0, "Event", "Event", "string", "data", None)
                    if len(self.events.items()) >= 1:
                        addEvLat = addEvLon = addEvEle = addEvDat = False
                        if "Event" in self.data.columns:
                            newcols = {}
                            if "Latitude" not in self.data.columns:
                                addEvLat = True
                                newcols["Latitude"] = np.nan
                                self.params["Latitude"] = PanParam(
                                    1600, "Latitude", "Latitude", "numeric", "event", "deg"
                                )
                            if "Longitude" not in self.data.columns:
                                addEvLon = True
                                newcols["Longitude"] = np.nan
                                self.params["Longitude"] = PanParam(
                                    1601, "Longitude", "Longitude", "numeric", "event", "deg"
                                )
                            if "Elevation" not in self.data.columns:
                                addEvEle = True
                                newcols["Elevation"] = np.nan
                                self.params["Elevation"] = PanParam(
                                    8128, "Elevation", "Elevation", "numeric", "event", "m"
                                )
                            if "Date/Time" not in self.data.columns:
                                addEvDat = True
                                newcols["Date/Time"] = pd.NaT
                                self.params["Date/Time"] = PanParam(
                                    1599, "Date/Time", "Date/Time", "datetime", "event", ""
                                )
                            if newcols:
                                self.data = pd.concat(
                                    [self.data, pd.DataFrame(newcols, index=self.data.index)],
                                    axis=1
                                )
                            for iev, pevent in self.events.items():
                                if pevent.latitude is not None and addEvLat:
                                    self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Latitude"].isnull()), ["Latitude"]] = self.events[iev].latitude
                                if pevent.longitude is not None and addEvLon:
                                    self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Longitude"].isnull()), ["Longitude"]] = self.events[iev].longitude
                                if pevent.elevation is not None and addEvEle:
                                    self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Elevation"].isnull()), ["Elevation"]] = self.events[iev].elevation
                                if pevent.datetime is not None and addEvDat:
                                    self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Date/Time"].isna()), ["Date/Time"]] = self.events[iev].datetime
                if self.deleteFlag != "":
                    if self.deleteFlag == "?" or self.deleteFlag == "*":
                        self.deleteFlag = "\\" + self.deleteFlag
                    self.data.replace(regex=r"^" + self.deleteFlag + "{1}.*", value="", inplace=True)
                self.data = self.data.dropna(axis=1, how="all")
                for paramcolumn in list(self.params.keys()):
                    if paramcolumn not in self.data.columns:
                        del self.params[paramcolumn]
                self.setQCDataFrame()
                self.data.replace(regex=r"^[\?/\*#\<\>]", value="", inplace=True)
                for col in self.data:
                    if col == "Date/Time":
                        continue
                    try:
                        self.data[col] = pd.to_numeric(self.data[col])
                    except (ValueError, TypeError):
                        pass
                try:
                    if "Date/Time" in self.data.columns:
                        self.data["Date/Time"] = pd.to_datetime(self.data["Date/Time"], format="ISO8601")
                except Exception as e:
                    self.data["Date/Time"] = pd.to_datetime(self.data["Date/Time"].replace({r"^.*([0-9]{4}){1}.*$": r"\1"}, regex=True), format="%Y", errors="coerce")
                    self.log(logging.WARNING, "Date/Time transformation failed: " + (str(e)))
                    pass
            except RequestException as exc:
                match getattr(exc.response, "status_code", -1):
                    case 401:
                        self.log(logging.WARNING, "Data access failed, a valid authentication token is needed")
                    case 406:
                        self.log(logging.WARNING, "Data access failed, no tabular data available")
                    case _:
                        self.log(logging.WARNING, f"Failed to load data: {exc}")

    def setQCDataFrame(self) -> None:
        tmp_qc_column_list = []
        try:
            for paramcolumn in list(self.params.keys()):
                if self.params[paramcolumn].type in ["numeric", "datetime"]:
                    tmp_qc_series = self.data[paramcolumn].astype(str).str.extract(r"(^[\*/\?])?(.+)")[0].astype(str)
                    tmp_qc_series.replace(to_replace={key: str(val) for key, val in self.quality_flag_replace.items()}, inplace=True)
                    tmp_qc_series = pd.to_numeric(tmp_qc_series, errors="coerce")
                    tmp_qc_column_list.append(tmp_qc_series)
            self.qcdata = pd.concat(tmp_qc_column_list, axis=1)
            self.qcdata = self.qcdata.dropna(how="all")
            self.qcdata.fillna(0, inplace=True)
        except Exception as e:
            self.log(logging.WARNING, "Could not create QC flag dataframe")

    def addQCParamsAndColumns(self, qc_suffix: str = "_QC", excludeColumns: list[str] | None = None) -> None:
        if excludeColumns is not None:
            joincolumns = list(set(self.data.columns) & set(set(self.qcdata.columns) - set(excludeColumns)))
        else:
            joincolumns = self.data.columns
        self.data = self.data.join(self.qcdata, rsuffix=qc_suffix)
        for paramcolumn in self.qcdata[joincolumns]:
            if self.params[paramcolumn].source == "data":
                ptype = "qc"
            else:
                ptype = "gqc"
            self.data[paramcolumn + qc_suffix].fillna(0)
            self.params[paramcolumn + qc_suffix] = PanParam(self.params[paramcolumn].id + 1000000000, self.params[paramcolumn].name + qc_suffix, self.params[paramcolumn].shortName + qc_suffix, source="pangaeapy", param_type=ptype)

    def getMetadataFormats(self) -> list[str]:
        formats = []
        linksetjson, linksetfilename = self.getMetadata("application/linkset+json")
        linksetdict = json.loads(linksetjson)
        if linksetdict.get("linkset"):
            for describedby in linksetdict["linkset"][0].get("describedby", []):
                formats.append(describedby.get("type"))
        return formats

    def getMetadata(self, accept: str = "application/vnd.pangaea.metadata+xml") -> tuple[str, str | None] | None:
        id = self.id
        try:
            r = requests.get(
                f"https://doi.pangaea.de/10.1594/PANGAEA.{id}",
                headers= {
                    "Accept": accept,
                    "Authorization": f"Bearer {self.auth_token}" if self.auth_token is not None else None,
                },
                timeout=(3.05, 10),
            )
            if r.status_code == 404:
                self.log(logging.ERROR, f"Data set does not exist, 404: {id}")
                return None
            r.raise_for_status()
            filename = None
            cd = r.headers.get("Content-Disposition")
            if cd:
                m = re.search(r'filename="?([^"]+)"?', cd)
                if m:
                    filename = m.group(1)
            return r.text, filename
        except RequestException as e:
            self.log(logging.ERROR, f"Failed to retrieve metadata information: {e}")
        except Exception as e:
            self.log(logging.ERROR, f"HTTP request error: {e}")
        return None

    def getGeometry(self) -> int | str:
        """
        Sometimes the topotype attribute has not been set correctly during the curation process.
        This method returns the real geometry (topographic type) of the dataset based on the x,y,z and t information of the data frame content.
        Still a bit experimental..
        """
        geotype = 0
        zgroup = ["Latitude", "Longitude"]
        tgroup = ["Latitude", "Longitude"]
        locgrp = ["Latitude", "Longitude"]
        p = pz = pt = len(self.data.groupby(locgrp))
        t = z = None
        if "Date_Time" in self.data.columns:
            t = "Date_Time"
        if "Depth_water" in self.data.columns:
            z = "Depth_water"
        elif "Depth" in self.data.columns:
            z = "Depth"
        elif "Depth_ice_snow" in self.data.columns:
            z = "Depth_ice_snow"
        elif "Depth_soil" in self.data.columns:
            z = "Depth_soil"
        if t is not None:
            tgroup.append(t)
            pt = len(self.data.groupby(tgroup))
        if z is not None:
            zgroup.append(z)
            pz = len(self.data.groupby(zgroup))
        if p == 1:
            if pt == 1 and pz == 1:
                geotype = "point"
            elif pt >= 1:
                if pz == 1 or len(self.events.items()) == 1:
                    geotype = "timeSeries"
                else:
                    geotype = "timeSeriesStack"
            else:
                geotype = "profile"
        else:
            if p == pz:
                geotype = "trajectory"
            elif pt > pz:
                geotype = "timeSeriesProfile"
            else:
                geotype = "trajectoryProfile"
        return geotype

    def getParamDict(self) -> dict[str, list[str | None]]:
        """The method returns translates the parameter object list into a dictionary"""
        paramdict = {"shortName": [], "name": [], "unit": [], "type": [], "format": []}
        for param_key, param_value in self.params.items():
            paramdict["shortName"].append(param_key)
            paramdict["name"].append(param_value.name)
            paramdict["unit"].append(param_value.unit)
            paramdict["type"].append(param_value.type)
            paramdict["format"].append(param_value.format)
        return paramdict

    def info(self) -> None:
        """The method returns a set of basic information about the PANGAEA dataset"""
        print("Citation:")
        print(self.citation)
        print()
        print("Toptype:")
        print(self.topotype)
        print()
        print("Parameters:")
        paramdf = pd.DataFrame(self.getParamDict())
        print(paramdf)
        print()
        print("Data: (first 5 rows)")
        print(self.data.head(5))

    def rename_column(self, old_name: str, new_name: str) -> None:
        if self.params.get(old_name):
            self.params[new_name] = self.params.pop(old_name)
            self.params[new_name].name = new_name
            self.params[new_name].shortName = new_name
            self.data.rename(columns={old_name: new_name}, inplace=True)
            self.qcdata.rename(columns={old_name: new_name}, inplace=True)

    def to_netcdf(self, filelocation: str | os.PathLike | None = None, save: bool = True, type: str = "sdn") -> bool | None:
        """
        This method creates a NetCDF file using PANGAEA data. It offers two different flavors: SeaDataNet NetCDF and an
        experimental internal format using NetCDF 4 groups.
        Currently, the method only supports simple types such as timeseries and profiles.
        The method created files are named as follows: [PANGAEA ID]_[type].cf

        Parameters
        ----------
        filelocation : str
            Indicates the location (directory) where the NetCDF file will be saved
        type : str
            This parameter sets the NetCDF profile type. Allowed values are 'sdn' (SeaDataNet) and 'pan' (PANGAEA style)
        save : Boolean
            If the file shall be saved on disk (filelocation or home directory/pan_export by default)
        """
        ret = None
        netcdfexporter = PanNetCDFExporter(self, filelocation=filelocation)
        if type == "sdn":
            netcdfexporter.renameSDNDimVars()
            netcdfexporter.pandataset.addQCParamsAndColumns(qc_suffix="_SEADATANET_QC", excludeColumns=["LATITUDE", "LONGITUDE", "TIME"])
        ret = netcdfexporter.create(style=type)
        if save:
            netcdfexporter.save()
        return ret

    def to_frictionless(self, filelocation: str | os.PathLike | None = None, save: bool = True) -> bool | None:
        """
        This method creates a frictionless data package (https://specs.frictionlessdata.io/data-package) file using PANGAEA metadata and data.
        A package will be saved as directory
        The method created directories are named as follows: [PANGAEA ID]_frictionless

        Parameters
        ----------
        filelocation : str
            Indicates the location (directory) where the frictionless file will be saved
        save : Boolean
            If the file shall be saved on disk (filelocation or home directory/pan_export by default)
        """
        frictionless_exporter = PanFrictionlessExporter(self, filelocation)
        ret = frictionless_exporter.create()
        if save:
            frictionless_exporter.save()
        return ret

    def to_dwca(self, save: bool = True) -> bool | None:
        """
        This method creates a Darwin Core Archive file using PANGAEA metadata and data.
        A package will be saved as directory
        The method created directories are named as follows: [PANGAEA ID]_dwca

        Parameters
        ----------
        filelocation : str
            Indicates the location (directory) where the DwC-A file will be saved
        save : Boolean
            If the file shall be saved on disk (filelocation or home directory/pan_export by default)
        """
        dwca_exporter = PanDarwinCoreArchiveExporter(self)
        ret = dwca_exporter.create()
        if save:
            dwca_exporter.save()
        self.logging.extend(dwca_exporter.logging)
        return ret

    def download(self, indices: list[int] | None = None, columns: list[str] | None = None, timeout: int = 600) -> list[Path | None]:
        """Download binary data if available; otherwise, save dataframe as CSV.

        Downloads can be very large. Consider explicitly defining the pangaeapy cache when calling PanDataSet.

        Parameters
        ----------
        indices : list
            Row indices of the data to download (e.g. [1, 2, 6]).
        columns : list of strings
            Column names of the data to download (e.g. ["Binary", "netCDF"]).
        timeout : int, optional
            Timeout in seconds for HTTP requests during download. Default is 600 seconds (10 minutes).
            For very large downloads that may take longer to complete, this can be increased.

        Returns
        -------
            List of downloaded or saved filenames
        """
        if self.data.empty:
            raise ValueError(f"Dataset has no available data to download!\n"
                             f"Check {self.doi} for more information on dataset.")
        binary_columns = ["binary", "netcdf", "image", "video", "text", "url", "csv"]
        pattern = "(?i)^(" + "|".join(binary_columns) + r")(?!.*\()"
        column_names = self.data.filter(regex=pattern).columns.tolist()
        if column_names:
            self.log(logging.INFO, f"Downloading files to {self.cachedir}")
            self.columns = columns if columns else column_names
            self.data_index = indices if indices else []
            self.timeout = timeout
            if not all([x in column_names for x in self.columns]):
                raise ValueError(f"Not all given columns ({self.columns}) are available!\n"
                                 f"Please select one or all of {column_names}.")
            if any([x >= self.data.shape[0] for x in self.data_index]):
                raise ValueError(f"Index out of range!\n"
                                 f"Possible index range: 0 - {self.data.shape[0]}.")
            harvester = PanDataHarvester(self)
            return harvester.run_download()
        else:
            self.log(logging.INFO, "Info: No binary data available.")
            self.log(logging.INFO, f"The dataset will be saved as a CSV file to {self.cachedir}")
            csv_path = Path(self.cachedir, f"{self.id}_data.csv")
            self.data.to_csv(csv_path, index=False)
            print(f"Dataset saved to {csv_path}")
            return [csv_path]
