import asyncio
import datetime
import io
import json
import logging
from pathlib import Path, PurePosixPath
import pickle
import re
import sqlite3 as sl
import textwrap
import time
from urllib.parse import unquote, urlparse
import zipfile

import aiohttp
import lxml.etree as ET
import numpy as np
import pandas as pd
import requests

from pangaeapy._core import CURRENT_VERSION, get_request, get_xml_content
from pangaeapy.exporter.pan_dwca_exporter import PanDarwinCoreAchiveExporter
from pangaeapy.exporter.pan_frictionless_exporter import PanFrictionlessExporter
from pangaeapy.exporter.pan_netcdf_exporter import PanNetCDFExporter

logger = logging.getLogger(__name__)


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

    def __init__(self, id, name, terms = []):
        self.id = id
        self.name = name
        self.terms = terms


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
    def __init__(self, label, name, URL=None, awardURI=None, id=None):
        self.label = label
        self.name = name
        self.URL = URL
        self.awardURI = awardURI
        self.id = id

class PanLicence:
    """PANGAEA Licence Class
    This class contains the licence information for each dataset
    """
    def __init__(self, label, name, URI=None):
        self.label = label
        self.name = name
        self.URI = URI

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
    def __init__(self, lastname, firstname=None, orcid=None, id=None, affiliations=None):
        self.lastname = lastname
        self.firstname = firstname
        self.fullname = self.lastname
        self.ORCID = orcid
        self.id = id
        if affiliations:
            self.affiliations = affiliations
        if firstname:
            self.fullname += ", " + self.firstname

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
    """
    def __init__(self,
                 label,
                 latitude=None,
                 longitude=None,
                 latitude2=None,
                 longitude2=None,
                 elevation=None,
                 datetime=None,
                 datetime2=None,
                 basis=None,
                 location=None,
                 campaign=None,
                 id = None,
                 method = None):
        self.label = label
        self.id = id
        if latitude is not None:
            self.latitude = float(latitude)
        else:
            self.latitude = None
        if longitude is not None:
            self.longitude = float(longitude)
        else:
            self.longitude = None
        if latitude2 is not None:
            self.latitude2 = float(latitude2)
        else:
            self.latitude2 = None
        if longitude2 is not None:
            self.longitude2 = float(longitude2)
        else:
            self.longitude2 = None
        if elevation is not None:
            self.elevation = float(elevation)
        else:
            self.elevation = None
        if isinstance(method, PanMethod):
            self.device = method.name
            self.deviceid = method.id
            self.method = method
        else:
            self.device, self.deviceid, self.method = None, None, None
        self.basis = basis
        # -- NEED TO CARE ABOUT datetime2!!!
        self.datetime = datetime
        self.datetime2 = datetime2
        self.location = location
        self.campaign = campaign

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
    def __init__(self, name=None, URI=None, callSign=None, IMOnumber=None):
        self.name = name
        self.URI = URI
        self.callSign = callSign
        self.IMOnumber = IMOnumber

class PanCampaign:
    """PANGAEA Campaign class
    The campaign during which the event took place, e.g. a ship cruise or observatory deployment

    name : str
        The name of the campaign
    URI : str
        The campaign URI
    start : str
        The start date of the campaign
    end : str
        The end date of the campaign
    startlocation : str
        The start location of the campaign
    endlocation : str
        The end location of the campaign
    BSHID : str
        The BSH ID for the campaign
    expeditionprogram: str
        URL of the campaign report
    """
    def __init__(self, name=None, URI=None, start=None, end=None, startlocation=None, endlocation=None, BSHID=None, expeditionprogram=None):
        self.name = name
        self.URI = URI
        self.start = start
        self.end = end
        self.startlocation = startlocation
        self.endlocation = endlocation
        self.BSHID = BSHID
        self.expeditionprogram = expeditionprogram

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
    def __init__(self, id, name, shortName, param_type, source, unit = None, unit_id = None, format = None, terms = [], comment = None, PI = dict(), dataseries = None, colno = None, method = None):
        self.id = id
        self.methodid = None
        self.method = method
        if isinstance(method, PanMethod):
            self.methodid = method.id
        self.name=name
        self.shortName=shortName
        # Synonym namespace dict predefined keys are CF: CF variables (), OS:OceanSites, SD:SeaDataNet abbreviations (TEMP, PSAL etc..)
        ns=('CF','OS','SD')
        self.synonym=dict.fromkeys(ns)
        self.type=param_type
        self.source=source
        self.unit=unit
        self.unit_id = unit_id
        self.format=format
        self.terms =terms
        self.comment = comment
        self.PI = PI
        self.dataseries = dataseries
        self.colno = colno

    def addSynonym(self,ns, name, uri=None, id=None, unit=None, unit_id = None):
        """
        Creates a new synonym for a parameter which is valid within the given name space. Synonyms are stored in the synonym attribute which is a dictionary

        Parameters
        ----------
        name : str
            the name of the synonym
        id : str
            the internal ID of the term used at its auhority ontology
        uri : str
            the URI (ideally actionable) leading to its auhority ontology entry/web page
        ns : str
            the namespace indicator for the sysnonym
        unit : str
            in case another unit expression is used for the synonym parameter within its namespace
        unit_id : str
            the id, UI or URN for the unit
        """
        self.synonym[ns] = {"name": name, "id": id, "uri": uri, "unit": unit, "unit_id": unit_id}

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
    def __init__(self, id=None, paramlist=None, deleteFlag='', enable_cache=False,
                 cachedir=None, include_data=True, expand_terms=[],
                 auth_token=None, cache_expiry_days=1):
        self.module_dir = Path(__file__).parent
        self.id = None
        self.uri, self.doi = "",""  # the doi
        self.logging = []
        self.logger = logger
        self._xml_root = None
        ### The constructor allows the initialisation of a PANGAEA dataset object either by using an integer dataset id or a DOI
        self.setID(id)
        self.ns = {"md": "http://www.pangaea.de/MetaData"}
        # Mapping should be moved to e.g. netCDF class/module??
        #moddir = os.path.dirname(os.path.abspath(__file__))
        #self.CFmapping=pd.read_csv(moddir+'\\PANGAEA_CF_mapping.txt',delimiter='\t',index_col='ID')
        # setting up the cache directory in the users home folder if no path is given
        if cachedir is None:
            self.cachedir = Path(Path.home(), ".pangaeapy_cache")
        else:
            self.cachedir = Path(cachedir)
        self.cachedir.mkdir(parents=True, exist_ok=True)
        self.cache = enable_cache
        self.cache_expiry_days = cache_expiry_days
        self.isCollection = False
        self.params = dict()
        self.parameters = self.params
        self.defaultparams = ["Latitude", "Longitude", "Event", "Elevation", "Date/Time"]
        self.paramlist = paramlist
        self.paramlist_index = []
        self.events = []
        self.projects = []
        self.licence = None
        # allowed geocodes for netcdf generation which are used as xarray dimensions not needed in the moment
        self._geocodes = {1599: "Date_Time", 1600: "Latitude", 1601: "Longitude", 1619: "Depth water"}
        self.data = pd.DataFrame()
        self.qcdata = pd.DataFrame()
        self.title = None
        self.abstract = None
        self.comment = None
        self.moratorium = None
        self.curationlevel = None
        self.processinglevel = None
        self.registrystatus = None
        self.citation = None
        self.year = None
        self.date = None
        self.mintimeextent = None
        self.maxtimeextent = None
        self.geometryextent = {}

        self.topotype = None
        self.authors = []
        self.terms_cache = {}  # temporary cache for terms
        self.terms_conn = sl.connect(Path(self.cachedir, "terms.db"))
        self.supplement_to = {}  # If this dataset is supllementary to another publication, give that publications title and URI here.
        self.relations = []  # list of relations as given in
        self.keywords = []  # list of keywords
        try:
            sql = "create table if not exists terms (term_id integer PRIMARY KEY, term_name text, term_json text, entry_date datetime default current_timestamp)"
            self.terms_conn.execute(sql)
            self.terms_conn.commit()
        except Exception as e:
            print("DB INIT ERROR: " + str(e))
        # replacing error list

        self.allowNetCDF = True
        self.eventInMatrix = False
        self.deleteFlag = deleteFlag
        self.collection_members = []
        self.include_data = include_data
        if isinstance(expand_terms, int):
            expand_terms = [expand_terms]
        if not isinstance(expand_terms, list):
            expand_terms = []
        self.expand_terms = expand_terms
        self.metaxml = None
        self.auth_token = auth_token

        # no symbol = valid(default)
        # ? = questionable(?0.345)
        # / = not valid( / 23.56)
        # * = unknown(*0.999)
        # = individual definition (#999)
        #self.logger.info('Test')
        self.quality_flags={'ok':'valid','?':'questionable','/':'not_valid','*':'unknown'}
        self.quality_flag_replace={'ok':0,'?':1,'/':2,'*':3}
        if self.id is not None:
            gotData = False

            if self.cache:
                # self.logging.append({'INFO':'Caching activated..trying to load data and metadata from cache'})
                self.log(logging.INFO, "Caching activated..trying to load data and metadata from cache")
                if self.check_pickle():
                    gotData = self.from_pickle()
                else:
                    self.drop_pickle()
                    gotData = False
            else:
                # delete existing cache
                self.drop_pickle()
            if not gotData:
                # print('trying to load data and metadata from PANGAEA')
                # check if title is already there, otherwise load metadata
                if not self.title:
                    self.setMetadata()
                if (self.loginstatus == "unrestricted" or self.auth_token) and not self.isCollection:
                    self.setData()
                    self.defaultparams = [s for s in self.defaultparams if s in self.params.keys()]
                    if self.paramlist is not None:
                        if len(self.paramlist) != len(self.paramlist_index):
                            # self.logging.append({'WARNING':'Inconsistent number of detected parameters, expected: '+str(len(self.paramlist))+' vs '+str(len(self.paramlist_index))})
                            self.log(logging.WARNING, "Inconsistent number of detected parameters, expected: " + str(len(self.paramlist)) + " vs " + str(len(self.paramlist_index)))
                    if self.cache:
                        self.to_pickle()
                else:
                    self.log(logging.WARNING, 'Dataset is either restricted or of type "collection"')
        else:
            # self.logging.append({'ERROR':'Dataset id missing, could not initialize PanDataSet object for: '+str(id)})
            self.log(logging.ERROR, "Dataset id missing, could not initialize PanDataSet object for: " + str(id))

    def log(self, level, message):
        message += " - " + str(self.doi)
        loglevel = logging.getLevelName(level)
        self.logging.append({loglevel: message})
        self.logger.log(level=level, msg=message)

    def get_pickle_path(self):
        dirs = textwrap.wrap(str(self.id).zfill(8), 2)
        dirpath = Path(self.cachedir, *dirs)
        return Path(dirpath, str(self.id) + "_data.pik")

    def check_pickle(self):
        """
        Verifies if a cached pickle files needs to be refreshed (reloaded)
        Files are checked after 24 hrs earliest but only updated in case the metadata indicates changes occured

        Parameters
        ----------
        expirydays

        Returns bool
        -------

        """
        ret = True
        pickle_location = self.get_pickle_path()
        if pickle_location.exists():
            pickle_time = pickle_location.stat().st_mtime
            if int(time.time()) - int(pickle_time) >= (self.cache_expiry_days * 86400):

                self.setMetadata()
                # 2016-10-08T05:40:17
                try:
                    # print('LAST UPDATE: ',self.doi, self.lastupdate, self.cache_expiry_days)
                    lastupdatets = time.mktime(datetime.datetime.strptime(self.lastupdate, "%Y-%m-%dT%H:%M:%S").timetuple())
                    # print(int(lastupdatets) , int(pickle_time))
                    if int(lastupdatets) >= int(pickle_time):
                        ret = False
                        # self.logging.append(
                        #    {'INFO': 'Dataset cache expired, refreshing cache'})
                        self.log(logging.INFO, "Dataset cache expired, refreshing cache")
                except Exception as e:
                    print(e)
                    ret = False
                    pass
            return ret

    def drop_pickle(self):
        self.get_pickle_path().unlink(missing_ok=True)

    def from_pickle(self):
        """
        Reads a PanDataSet object from a pickle file

        """
        ret = False
        pickle_path = self.get_pickle_path()
        if pickle_path.exists():
            try:
                with open(pickle_path, "rb") as f:
                    tmp_dict = pickle.load(f)
                tmp_dict["logging"] = []
                tmp_dict["_xml_root"] = ET.fromstring(tmp_dict["metaxml"].encode())
                self.__dict__.update(tmp_dict)
                # self.logging.append({'INFO':'Loading data and metadata from cache: '+str(pickle_path)})
                self.log(logging.INFO, "Loading data and metadata from cache: " + str(pickle_path))
                ret = True
            except:
                # self.logging.append({'WARNING':'Loading data and metadata from cache failed'})
                self.log(logging.WARNING, "Loading data and metadata from cache failed")
                ret = False
        else:
            ret = False
        return ret

    def to_pickle(self):
        """
        Writes a PanDataSet object to a pickle file

        """
        if not self.data.empty:
            state = self.__dict__.copy()
            del state["terms_conn"]
            del state["_xml_root"]
            pickle_path = self.get_pickle_path()
            try:
                pickle_path.parent.mkdir(parents=True)
            except Exception as e:
                pass
            with open(pickle_path, "wb") as f:
                pickle.dump(state, f, 2)
            # self.logging.append({'INFO': 'Saved cache (pickle) file at: ' + str(self.get_pickle_path())})
            self.log(logging.INFO, "Saved cache (pickle) file at: " + str(self.get_pickle_path()))
        else:
            self.log(logging.WARNING, "Skipped saving cache (pickle) since the dataset contains no data")

    def setID(self, id):
        """
        Initialize the ID of a data set in case it was not defined in the constructur
        Parameters
        ----------
        id : str
            The identifier of a PANGAEA dataset. An integer number or a DOI is accepted here
        """
        try:
            self.id = int(re.fullmatch(
                r"(?:(?:(?:https?://)?(?:dx\.)?doi\.org/|doi:)?10\.1594/PANGAEA\.)?(\d+)",
                str(id).strip(),
            ).group(1))
        except AttributeError:
            self.log(logging.ERROR, "Invalid Identifier or DOI: " + str(id))

    def _getIDParts(self, idstr):
        # returns dict extracted from panmd id strings e.g
        # col13.ds10866878.param7387
        ret = dict()
        if isinstance(idstr, str):
            idmatches = re.findall(r"([a-z]+)([0-9]+)", idstr)
            if idmatches:
                ret = dict(idmatches)
        return ret

    def _setEvents(self, panXMLEvents):
        """
        Initializes the list of Events from a metadata XML file for a given pangaea dataset.
        """
        for event in panXMLEvents:
            eventElevation = eventDateTime = eventDateTime2 = None
            eventDevice = eventLabel = None
            eventDeviceID = None
            basis_name = basis_URI = basis_callsign = basis_imonumber = None
            campaign_name = campaign_URI = campaign_start = campaign_end = None
            eventLongitude = eventLatitude = eventLatitude2 = eventLongitude2 = eventLocation = None
            startlocation = endlocation = BSHID = expeditionprogram = None

            eventID = self._getIDParts(event.get("id")).get("event")

            if event.find("md:elevation",self.ns) is not None:
                eventElevation=event.find("md:elevation",self.ns).text
            if event.find("md:dateTime",self.ns) is not None:
                eventDateTime= event.find("md:dateTime",self.ns).text
            if event.find("md:dateTime2",self.ns) is not None:
                eventDateTime2= event.find("md:dateTime2",self.ns).text
            if event.find("md:longitude",self.ns) is not None:
                eventLongitude= event.find("md:longitude",self.ns).text
            if event.find("md:latitude",self.ns) is not None:
                eventLatitude= event.find("md:latitude",self.ns).text
            if event.find("md:longitude2",self.ns) is not None:
                eventLongitude2= event.find("md:longitude2",self.ns).text
            if event.find("md:latitude2",self.ns) is not None:
                eventLatitude2= event.find("md:latitude2",self.ns).text
            if event.find("md:label",self.ns) is not None:
                eventLabel= event.find("md:label",self.ns).text
            if event.find("md:location/md:name",self.ns) is not None:
                eventLocation= event.find("md:location/md:name",self.ns).text
            if event.find("md:method/md:name",self.ns) is not None:
                eventDeviceTerms =[]
                eventDevice= event.find("md:method/md:name",self.ns).text
                eventDeviceID = self._getIDParts(event.find("md:method",self.ns).get("id")).get("method")
                for terminfo in event.findall("md:method/md:term", self.ns):
                    eventDeviceTerms.append(self._getTermInfo(terminfo))
                eventMethod = PanMethod(eventDeviceID, eventDevice,eventDeviceTerms)
            else:
                eventMethod = None
            if event.find("md:basis",self.ns) is not None:
                basis= event.find("md:basis",self.ns)
                if basis.find("md:name",self.ns) is not None:
                    basis_name= basis.find("md:name",self.ns).text
                else:
                    basis_name = None
                if basis.find("md:URI", self.ns) is not None:
                    basis_URI = basis.find("md:URI", self.ns).text
                else:
                    basis_URI = None
                if basis.find("md:callSign", self.ns) is not None:
                    basis_callsign = basis.find("md:callSign", self.ns).text
                else:
                    basis_callsign = None
                if basis.find("md:IMOnumber", self.ns) is not None:
                    basis_imonumber = basis.find("md:IMOnumber", self.ns).text
                else:
                    basis_imonumber = None
                eventBasis = PanBasis(basis_name, basis_URI, basis_callsign, basis_imonumber)
            else:
                eventBasis = None
            if event.find("md:campaign", self.ns) is not None:
                campaign = event.find("md:campaign", self.ns)
                if campaign.find("md:name", self.ns) is not None:
                    campaign_name = campaign.find("md:name", self.ns).text
                if campaign.find("md:URI", self.ns) is not None:
                    campaign_URI = campaign.find("md:URI", self.ns).text
                if campaign.find("md:start", self.ns) is not None:
                    campaign_start = campaign.find("md:start", self.ns).text
                if campaign.find("md:end", self.ns) is not None:
                    campaign_end = campaign.find("md:end", self.ns).text
                if campaign.find('md:attribute[@name="Start location"]', self.ns) is not None:
                    startlocation = campaign.find('md:attribute[@name="Start location"]', self.ns).text
                if campaign.find('md:attribute[@name="End location"]', self.ns) is not None:
                    endlocation = campaign.find('md:attribute[@name="End location"]', self.ns).text
                if campaign.find('md:attribute[@name="BSH ID"]', self.ns) is not None:
                    BSHID = campaign.find('md:attribute[@name="BSH ID"]', self.ns).text
                if campaign.find('md:attribute[@name="Expedition Program"]', self.ns) is not None:
                    expeditionprogram = campaign.find('md:attribute[@name="Expedition Program"]', self.ns).text
                else:
                    expeditionprogram = None
                eventCampaign = PanCampaign(campaign_name, campaign_URI, campaign_start, campaign_end, startlocation, endlocation, BSHID, expeditionprogram)
            else:
                eventCampaign = None

            self.events.append(PanEvent(eventLabel,
                                        eventLatitude,
                                        eventLongitude,
                                        eventLatitude2,
                                        eventLongitude2,
                                        eventElevation,
                                        eventDateTime,
                                        eventDateTime2,
                                        eventBasis,
                                        eventLocation,
                                        eventCampaign,
                                        eventID,
                                        eventMethod
                                        ))


    def _getExtendedTermInfo(self, termid):
        termJSON = None
        try:
            selsql = "select * from terms where term_id=" + str(termid)
            tres = self.terms_conn.execute(selsql)
            found_term = tres.fetchone()
            if found_term:
                termJSON = json.loads(found_term[2])
        except Exception as e:
            print("getTermInfo ERROR I : ", e)
        if not termJSON:
            try:
                termr = get_request(f"https://ws.pangaea.de/es/pangaea-terms/term/{termid}")
                termJSON = termr.json()
                term_name = termJSON["_source"]["name"]
                inssql = "insert into terms (term_id, term_name, term_json) values (?,?,?)"
                self.terms_conn.execute(inssql, (termid, term_name, json.dumps(termJSON)))
                self.terms_conn.commit()
            except Exception as e:
                print("getTermInfo ERROR II: ", e, inssql)
        return termJSON

    def _getTermInfo(self,terminfo, terminology_id = None):
        """
        Uses terms webservice to enrich the parameter info with linked terms and their classification
        terminology_id: restrict to given terminology
        """
        termret = {}
        termid = None
        termname = None
        if terminfo.find("md:name", self.ns) is not None:
            termname = terminfo.find("md:name", self.ns).text
            termidparts = self._getIDParts(str(terminfo.get("id")))
            if termidparts.get("term"):
                termid = int(termidparts.get("term"))
            terminologyid = int(terminfo.get("terminologyId"))
            termuri = terminfo.get("semanticURI")
        try:
            if self.expand_terms and (terminologyid in self.expand_terms):
                if isinstance(termid, int):
                    if termid not in self.terms_cache:
                        try:
                            termJSON = self._getExtendedTermInfo(termid)
                            if termJSON.get("_source"):
                                self.terms_cache[termid] = []
                                if termJSON["_source"].get("main_topics"):
                                    if isinstance(termJSON["_source"].get("main_topics"), list):
                                        self.terms_cache[termid].extend(termJSON["_source"].get("main_topics"))
                                    else:
                                        self.terms_cache[termid].append(termJSON["_source"].get("main_topics"))
                                if termJSON["_source"].get("topics"):
                                    if isinstance(termJSON["_source"].get("topics"), list):
                                        self.terms_cache[termid].extend(termJSON["_source"].get("topics"))
                                    else:
                                        self.terms_cache[termid].append(termJSON["_source"].get("topics"))
                        except Exception as e:
                            # self.logging.append({'WARNING': 'Failed loading and parsing PANGAEA Term JSON: ' + str(e)})
                            self.log(logging.WARNING, "Failed loading and parsing PANGAEA Term JSON: " + str(e))
            if self.expand_terms:
                if self.terms_cache.get(termid):
                    classification = self.terms_cache.get(termid)
                else:
                    classification = []
                # print(termid, classification)
                termret = {"id": termid, "name": str(termname), "semantic_uri": termuri, "ontology": terminologyid, "classification": classification}
            else:
                termret = {"id": termid, "name": str(termname), "semantic_uri": termuri, "ontology": terminologyid}
        except Exception as e:
            # self.logging.append({'WARNING': 'Failed to expand terms ' + (str(e))})
            self.log(logging.WARNING, "Failed to expand terms " + (str(e)))
        return termret

    def _setParameters(self, panXMLMatrixColumn):
        """
        Initializes the list of parameter objects from the metadata XML info
        """
        coln = dict()
        if panXMLMatrixColumn is not None:
            panGeoCode = []
            for matrix in panXMLMatrixColumn:
                panparCFName = None
                colno = matrix.get("col")
                paramstr = matrix.find("md:parameter", self.ns)
                # panparID=int(self._getID(str(paramstr.get('id'))))
                paramidparts = self._getIDParts(str(paramstr.get("id")))
                panparID = None
                dataseriesID = None
                if paramidparts.get("param"):
                    panparID = int(paramidparts.get("param"))
                elif paramidparts.get("geocode"):
                    panparID = int(paramidparts.get("geocode"))
                if paramidparts.get("ds"):
                    dataseriesID = int(paramidparts.get("ds"))
                panparShortName = ""
                if paramstr.find("md:shortName", self.ns) is not None:
                    panparShortName = paramstr.find("md:shortName", self.ns).text
                    panparIndex = panparShortName
                    # Rename duplicate column headers
                    if panparShortName in coln:
                        coln[panparShortName] += 1
                        panparIndex = panparShortName + "_" + str(coln[panparShortName])
                    else:
                        coln[panparShortName] = 1
                panparType=matrix.get("type")
                panparUnit = None
                if(paramstr.find("md:unit",self.ns) is not None):
                    panparUnit=paramstr.find("md:unit",self.ns).text
                panparComment=None
                if(matrix.find("md:comment",self.ns) is not None):
                    panparComment=matrix.find("md:comment",self.ns).text
                panparMethod = None
                if (matrix.find("md:method", self.ns) is not None):
                    panparMethodTerms = []
                    panparMethodName = ""
                    if (matrix.find("md:method/md:name", self.ns) is not None):
                        panparMethodName = matrix.find("md:method/md:name", self.ns).text
                    panparMethodID = self._getIDParts(matrix.find("md:method", self.ns).get('id')).get('method')
                    for pmterminfo in matrix.findall("md:method/md:term", self.ns):
                        panparMethodTerms.append(self._getTermInfo(pmterminfo))
                    panparMethod = PanMethod(panparMethodID, panparMethodName,panparMethodTerms)
                panparPI = None
                panparPI_firstname, panparPI_lastname = None, None
                if matrix.find("md:PI", self.ns) is not None:
                    panparPI_ID = self._getIDParts(matrix.find("md:PI", self.ns).get("id")).get("pi")
                    if matrix.find("md:PI/md:firstName", self.ns) is not None:
                        panparPI_firstname = matrix.find("md:PI/md:firstName", self.ns).text
                    panparPI_lastname = matrix.find("md:PI/md:lastName", self.ns).text
                    panparPI_fullname = ", ".join(filter(None, [panparPI_lastname, panparPI_firstname]))
                    panparPI = {"id": panparPI_ID, "name": panparPI_fullname, "last_name": panparPI_lastname, "first_name": panparPI_firstname}
                panparFormat = matrix.get("format")
                if panparShortName == "Event":
                    self.eventInMatrix = True
                # if panparID in self.CFmapping.index:
                #    panparCFName=self.CFmapping.at[panparID,'STDNAME']
                # Add information about terms/ontologies used:
                termlist = []
                for terminfo in paramstr.findall("md:term", self.ns):
                    termlist.append(self._getTermInfo(terminfo))
                self.params[panparIndex] = PanParam(id=panparID,name=paramstr.find('md:name',self.ns).text,shortName=panparShortName,param_type=panparType,source=matrix.get('source'),unit=panparUnit,format=panparFormat,terms=termlist, comment=panparComment,PI =panparPI, dataseries = dataseriesID, colno = colno, method = panparMethod)
                self.parameters = self.params
                if panparType == "geocode":
                    if panparShortName in panGeoCode:
                        self.allowNetCDF = False
                        # self.logging.append({'WARNING': 'Data set contains duplicate Geocodes'})
                        self.log(logging.WARNING, "Data set contains duplicate Geocodes")
                    else:
                        panGeoCode.append(panparShortName)

    def getEventsAsFrame(self):
        """
        For more convenient handling of event info, this method returns a dataframe containing all events with their attributes as columns
        Please note that this version just takes campaign names, not other campaign attributes
        """
        df = pd.DataFrame()
        try:
            df = pd.DataFrame([ev.__dict__ for ev in self.events])
            df["campaign"] = df["campaign"].apply(lambda x: x.name)
        except:
            pass
        return df

    def setData(self, addEventColumns=True):
        """
        This method populates the data DataFrame with data from a PANGAEA dataset.
        In addition to the data given in the tabular ASCII file delivered by PANGAEA.

        Parameters
        ----------
        addEventColumns : boolean
            In case Latitude, Longitude, Elevation, Date/Time and Event are not given in the ASCII matrix, which sometimes is possible in single Event datasets,
            the setData could add these columns to the dataframe using the information given in the metadata for Event. Default is 'True'

        """
        # converting list of parameters` short names (from user input) to the list of parameters` indexes
        # the list of parameters` indexes is an argument for pd.read_csv
        if self.paramlist is not None:
            self.paramlist += self.defaultparams
            for parameter in self.paramlist:
                iter = 0
                for shortName in self.params.keys():
                    if parameter == shortName:
                        self.paramlist_index.append(iter)
                    iter += 1
            if len(self.paramlist) != len(self.paramlist_index):
                # self.logging.append({'WARNING': 'Error entering parameters`short names, inconsitent number of parameters'})
                self.log(logging.WARNING, "Error entering parameters`short names, inconsitent number of parameters")
        else:
            self.paramlist_index = None
        if self.include_data:
            try:
                dataResponse = get_request(
                    f"https://doi.pangaea.de/10.1594/PANGAEA.{self.id}",
                    accepted_type="text/tab-separated-values",
                    auth_token=self.auth_token,
                )

                panDataTxt = dataResponse.text
                if int(dataResponse.status_code) == 200:
                    if "text" in str(dataResponse.headers.get("Content-Type")):
                        panData = re.sub(r"/\*(.*)\*/", "", panDataTxt, count=1, flags=re.DOTALL).strip()
                        # Read in PANGAEA Data
                        self.data = pd.read_csv(io.StringIO(panData), index_col=False, on_bad_lines="skip", sep="\t", usecols=self.paramlist_index, names=list(self.params.keys()), skiprows=[0])
                        # add geocode/dimension columns from Event

                        # if addEventColumns==True and self.topotype!="not specified":
                        if addEventColumns:
                            if len(self.events) == 1:
                                if "Event" not in self.data.columns:
                                    self.data["Event"] = self.events[0].label
                                    self.params["Event"] = PanParam(0, "Event", "Event", "string", "data", None)
                            if len(self.events) >= 1:
                                addEvLat = addEvLon = addEvEle = addEvDat = False
                                if "Event" in self.data.columns:
                                    if "Latitude" not in self.data.columns:
                                        addEvLat = True
                                        self.data["Latitude"] = np.nan
                                        self.params["Latitude"] = PanParam(1600, "Latitude", "Latitude", "numeric", "event", "deg")
                                    if "Longitude" not in self.data.columns:
                                        addEvLon = True
                                        self.data["Longitude"] = np.nan
                                        self.params["Longitude"] = PanParam(1601, "Longitude", "Longitude", "numeric", "event", "deg")
                                    if "Elevation" not in self.data.columns:
                                        addEvEle = True
                                        self.data["Elevation"] = np.nan
                                        self.params["Elevation"] = PanParam(8128, "Elevation", "Elevation", "numeric", "event", "m")
                                    if "Date/Time" not in self.data.columns:
                                        addEvDat = True
                                        self.data["Date/Time"] = "NaN"
                                        self.params["Date/Time"] = PanParam(1599, "Date/Time", "Date/Time", "datetime", "event", "")
                                    for iev, pevent in enumerate(self.events):
                                        if pevent.latitude is not None and addEvLat:
                                            self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Latitude"].isnull()), ["Latitude"]] = self.events[iev].latitude
                                        if pevent.longitude is not None and addEvLon:
                                            self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Longitude"].isnull()), ["Longitude"]] = self.events[iev].longitude
                                        if pevent.elevation is not None and addEvEle:
                                            self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Elevation"].isnull()), ["Elevation"]] = self.events[iev].elevation
                                        if pevent.datetime is not None and addEvDat:
                                            self.data.loc[(self.data["Event"] == pevent.label) & (self.data["Date/Time"] == "NaN"), ["Date/Time"]] = str(self.events[iev].datetime)
                        # -- delete values with given QC flags
                        if self.deleteFlag != "":
                            if self.deleteFlag == "?" or self.deleteFlag == "*":
                                self.deleteFlag = "\\" + self.deleteFlag
                            self.data.replace(regex=r"^" + self.deleteFlag + "{1}.*", value="", inplace=True)

                        # --- Delete empty columns
                        self.data = self.data.dropna(axis=1, how="all")
                        # --- Preserve QC Flags in self.qcdata DataDrame and
                        # --- Replace Quality Flags for numeric columns

                        for paramcolumn in list(self.params.keys()):
                            if paramcolumn not in self.data.columns:
                                del self.params[paramcolumn]

                        self.setQCDataFrame()
                        self.data.replace(regex=r"^[\?/\*#\<\>]", value="", inplace=True)

                        # --- Adjust Column Data Types
                        for col in self.data:
                            try:
                                self.data[col] = pd.to_numeric(self.data[col])
                            except (ValueError, TypeError):
                                pass
                        try:
                            if "Date/Time" in self.data.columns:
                                self.data["Date/Time"] = pd.to_datetime(self.data["Date/Time"], format="ISO8601")
                                # self.data['Date/Time'] = pd.to_datetime(self.data['Date/Time'], format='%Y-%m-%dT%H:%M:%S')
                        except Exception as e:
                            # try to preserve the year at least:
                            self.data["Date/Time"] = pd.to_datetime(self.data["Date/Time"].replace({r"^.*([0-9]{4}){1}.*$": r"\1"}, regex=True), format="%Y", errors="coerce")
                            self.log(logging.WARNING, "Date/Time transformation failed: " + (str(e)))
                            pass

                    else:
                        # self.logging.append({'WARNING': 'Dataset seems to be a binary file which cannot be handled by pangaeapy'})
                        self.log(logging.WARNING, "Dataset seems to be a binary file which cannot be handled by pangaeapy")
                elif int(dataResponse.status_code) == 401:
                    if self.auth_token:
                        # self.logging.append({'WARNING': 'Data access failed, invalid auth token'})
                        self.log(logging.WARNING, "Data access failed, invalid auth token")
                    else:
                        # self.logging.append({'WARNING': 'Data access failed, authorisation failed '})
                        self.log(logging.WARNING, "Data access failed, authorisation failed ")
                elif int(dataResponse.status_code) == 406:
                    self.log(logging.WARNING, "Data access failed, no tabular data available")
                else:
                    # self.logging.append({'WARNING': 'Data access failed, response code '+(str(dataResponse.status_code))})
                    self.log(logging.WARNING, "Data access failed, response code " + (str(dataResponse.status_code)))
            except Exception as e:
                # self.logging.append({'ERROR':'Loading data failed, reason: '+str(e)})
                self.log(logging.ERROR, "Loading data failed, reason: " + str(e))

    def setQCDataFrame(self):
        tmp_qc_column_list = []
        try:
            for paramcolumn in list(self.params.keys()):
                if self.params[paramcolumn].type in ["numeric", "datetime"]:
                    tmp_qc_series = self.data[paramcolumn].astype(str).str.extract(r"(^[\*/\?])?(.+)")[0].astype(str)
                    # self.qcdata[paramcolumn] = self.data[paramcolumn].astype(
                    #    str).str.extract(r'(^[\*/\?])?(.+)')[0]
                    # self.qcdata[paramcolumn].fillna(value='ok', inplace=True)
                    tmp_qc_series.replace(to_replace={key: str(val) for key, val in self.quality_flag_replace.items()}, inplace=True)
                    tmp_qc_series = pd.to_numeric(tmp_qc_series, errors="coerce")
                    tmp_qc_column_list.append(tmp_qc_series)
                    # self.qcdata[paramcolumn].replace(to_replace=self.quality_flag_replace, inplace=True)
            self.qcdata = pd.concat(tmp_qc_column_list, axis=1)
            self.qcdata = self.qcdata.dropna(how="all")
            self.qcdata.fillna(0, inplace=True)
        except Exception as e:
            # self.logging.append({'WARNING': 'Could not create QC flag dataframe'})
            self.log(logging.WARNING, "Could not create QC flag dataframe")

    def addQCParamsAndColumns(self, qc_suffix="_QC", excludeColumns=[]):
        # self.data.replace(regex=r'^[\?/\*#\<\>]', value='', inplace=True)
        if excludeColumns:
            joincolumns = list(set(self.data.columns) & set(set(self.qcdata.columns) - set(excludeColumns)))
        else:
            joincolumns = self.data.columns
        self.data = self.data.join(self.qcdata, rsuffix=qc_suffix)
        for paramcolumn in self.qcdata[joincolumns]:
            if self.params[paramcolumn].source == "data":
                ptype = "qc"
            else:
                # geocodeqc
                ptype = "gqc"
            self.data[paramcolumn + qc_suffix].fillna(0)
            self.params[paramcolumn + qc_suffix] = PanParam(self.params[paramcolumn].id + 1000000000, self.params[paramcolumn].name + qc_suffix, self.params[paramcolumn].shortName + qc_suffix, source="pangaeapy", param_type=ptype)

    def _setCitation(self):
        r = get_request(
            f"https://doi.pangaea.de/10.1594/PANGAEA.{self.id}",
            accepted_type="text/x-bibliography",
            auth_token=self.auth_token,
        )
        if r.status_code != 404:
            self.citation = r.text
        else:
            # self.logging.append({'WARNING':'Could not retrieve citation info from PANGAEA'})
            self.log(logging.WARNING, "Could not retrieve citation info from PANGAEA")

    def find(self, path, key=None):
        """Find a single XML node.

        Parameters
        ----------
        path : str
            XPath expression to search for
        key : str
            node attribute to fetch instead of its text

        Returns
        -------
        str or None
            node attribute or text
        """
        return get_xml_content(self._xml_root, path, namespaces=self.ns, key=key)

    def findall(self, path, key=None):
        """Find XML nodes.

        Parameters
        ----------
        path : str
            XPath expression to search for
        key : str
            node attribute to fetch instead of its text

        Returns
        -------
        list of str or None
            list of node attributes or texts
        """
        return get_xml_content(self._xml_root, path, namespaces=self.ns, key=key, multiple=True)

    @property
    def datastatus(self):
        return self.find('./md:technicalInfo/md:entry[@key="status"]', key="value")

    @property
    def lastupdate(self):
        return self.date if (val := self.find('./md:technicalInfo/md:entry[@key="lastModified"]', key="value")) is None else val

    @property
    def loginstatus(self):
        return "unrestricted" if (val := self.find('./md:technicalInfo/md:entry[@key="loginOption"]', key="value")) is None else val

    def setMetadata(self):
        """
        The method initializes the metadata of the PanDataSet object using the information of a PANGAEA metadata XML file.

        """
        r = None
        try:
            r = get_request(
            f"https://doi.pangaea.de/10.1594/PANGAEA.{self.id}",
            accepted_type="application/vnd.pangaea.metadata+xml",
            auth_token=self.auth_token,
        )
        except Exception as e:
            self.log(logging.ERROR, "HTTP request error: " + str(e))
        if r is not None:
            if r.status_code != 404:
                try:
                    r.raise_for_status()
                    xmlText = r.text
                    self.metaxml = xmlText
                    xml = ET.fromstring(xmlText.encode())
                    self._xml_root = xml
                    if self.datastatus not in ["deleted", None]:
                        if self.loginstatus != "unrestricted":
                            if self.auth_token:
                                # self.logging.append({'INFO': 'Trying to load protected dataset using the given auth token'})
                                self.log(logging.INFO, "Trying to load protected dataset using the given auth token")
                            else:
                                # self.logging.append({'WARNING': 'Data set is protected'})
                                self.log(logging.WARNING, "Data set is protected")
                        if xml.find('./md:technicalInfo/md:entry[@key="collectionType"]', self.ns) is not None:
                            self.log(logging.WARNING, "Data set is of type collection, please select one of its child datasets")
                            self.isCollection = True
                            self.collection_members = [
                                f"doi:10.1594/PANGAEA.{el[1:]}"
                                for el in xml.find(
                                    './md:technicalInfo/md:entry[@key="collectionChilds"]', self.ns
                                ).get("value").split(",")
                            ]

                        """hierarchyLevel=xml.find('./md:technicalInfo/md:entry[@key="hierarchyLevel"]',self.ns)
                        if hierarchyLevel!=None:
                            if hierarchyLevel.get('value')=='parent':
                                #self.logging.append({'WARNING':'Data set is of type parent, please select one of its child datasets'})
                                self.log(logging.WARNING, 'Data set is of type collection, please select one of its child datasets')
                                self.isCollection=True
                                self._setCollectionMembers()"""
                        self.title = xml.find("./md:citation/md:title", self.ns).text
                        if xml.find("./md:abstract", self.ns) is not None:
                            self.abstract = xml.find("./md:abstract", self.ns).text
                        self.registrystatus = xml.find('./md:technicalInfo/md:entry[@key="DOIRegistryStatus"]', self.ns).get("value")
                        if xml.find('./md:technicalInfo/md:entry[@key="moratoriumUntil"]', self.ns) is not None:
                            self.moratorium = xml.find('./md:technicalInfo/md:entry[@key="moratoriumUntil"]', self.ns).get("value")
                        if xml.find("./md:status/md:curationLevel/md:name", self.ns) is not None:
                            self.curationlevel = xml.find("./md:status/md:curationLevel/md:name", self.ns).text
                        if xml.find("./md:status/md:processingLevel/md:name", self.ns) is not None:
                            self.processinglevel = xml.find("./md:status/md:processingLevel/md:name", self.ns).text
                        if xml.find("./md:citation/md:year", self.ns) is not None:
                            self.year = xml.find("./md:citation/md:year", self.ns).text
                        if xml.find("./md:citation/md:dateTime", self.ns) is not None:
                            self.date = xml.find("./md:citation/md:dateTime", self.ns).text
                        self.doi = self.uri = xml.find("./md:citation/md:URI", self.ns).text
                        # extent
                        if xml.find("./md:extent/md:temporal/md:minDateTime", self.ns) is not None:
                            self.mintimeextent = xml.find("./md:extent/md:temporal/md:minDateTime", self.ns).text
                        if xml.find("./md:extent/md:temporal/md:maxDateTime", self.ns) is not None:
                            self.maxtimeextent = xml.find("./md:extent/md:temporal/md:maxDateTime", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:westBoundLongitude", self.ns) is not None:
                            self.geometryextent["westBoundLongitude"] = xml.find("./md:extent/md:geographic/md:westBoundLongitude", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:eastBoundLongitude", self.ns) is not None:
                            self.geometryextent["eastBoundLongitude"] = xml.find("./md:extent/md:geographic/md:eastBoundLongitude", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:southBoundLatitude", self.ns) is not None:
                            self.geometryextent["southBoundLatitude"] = xml.find("./md:extent/md:geographic/md:southBoundLatitude", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:northBoundLatitude", self.ns) is not None:
                            self.geometryextent["northBoundLatitude"] = xml.find("./md:extent/md:geographic/md:northBoundLatitude", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:northBoundLatitude", self.ns) is not None:
                            self.geometryextent["northBoundLatitude"] = xml.find("./md:extent/md:geographic/md:northBoundLatitude", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:meanLongitude", self.ns) is not None:
                            self.geometryextent["meanLongitude"] = xml.find("./md:extent/md:geographic/md:meanLongitude", self.ns).text
                        if xml.find("./md:extent/md:geographic/md:meanLatitude", self.ns) is not None:
                            self.geometryextent["meanLatitude"] = xml.find("./md:extent/md:geographic/md:meanLatitude", self.ns).text

                        topotypeEl = xml.find("./md:extent/md:topoType", self.ns)
                        if topotypeEl is not None:
                            self.topotype = topotypeEl.text
                        else:
                            self.topotype = None
                        for author in xml.findall("./md:citation/md:author", self.ns):
                            lastname = None
                            firstname = None
                            orcid = None
                            if author.find("md:lastName", self.ns) is not None:
                                lastname = author.find("md:lastName", self.ns).text
                            if author.find("md:firstName", self.ns) is not None:
                                firstname = author.find("md:firstName", self.ns).text
                            if author.find("md:orcid", self.ns) is not None:
                                orcid = author.find("md:orcid", self.ns).text
                            # if author.find("md:affiliation", self.ns)!=None:
                            authoraffiliations = []
                            for affiliations in author.findall("md:affiliation", self.ns):
                                afm = re.search(r"\.inst([0-9]+)$", str(affiliations.get("id")))
                                if afm:
                                    authoraffiliations.append(afm[1])
                            # print(authoraffiliations)
                            authorid = author.get("id")
                            if authorid:
                                authorid = int(authorid.replace("dataset.author", ""))
                            self.authors.append(PanAuthor(lastname, firstname, orcid, authorid, authoraffiliations))
                        for project in xml.findall("./md:project", self.ns):
                            label = None
                            name = None
                            URI = None
                            awardURI = None
                            if project.find("md:label", self.ns) is not None:
                                label = project.find("md:label", self.ns).text
                            if project.find("md:name", self.ns) is not None:
                                name = project.find("md:name", self.ns).text
                            if project.find("md:URI", self.ns) is not None:
                                URI = project.find("md:URI", self.ns).text
                            if project.find("md:award/md:URI", self.ns) is not None:
                                awardURI = project.find("md:award/md:URI", self.ns).text
                            if project.get("id"):
                                projectid = str(project.get("id")).replace("project", "")
                            self.projects.append(PanProject(label, name, URI, awardURI, int(projectid)))
                        if xml.find("./md:license", self.ns) is not None:
                            license = xml.find("./md:license", self.ns)
                            label = None
                            name = None
                            URI = None
                            if license.find("md:label", self.ns) is not None:
                                label = license.find("md:label", self.ns).text
                            if license.find("md:name", self.ns) is not None:
                                name = license.find("md:name", self.ns).text
                            if license.find("md:URI", self.ns) is not None:
                                URI = license.find("md:URI", self.ns).text
                            self.licence = PanLicence(label, name, URI)
                        if xml.find("./md:comment", self.ns) is not None:
                            self.comment = xml.find("./md:comment", self.ns).text
                        for reference in xml.findall("./md:reference", self.ns):
                            refURI = None
                            reftitle = None
                            reftype = reference.get("relationType")
                            refid = reference.get("id")
                            if reference.find("md:URI", self.ns) is not None:
                                refURI = reference.find("md:URI", self.ns).text
                            if reference.find("md:title", self.ns) is not None:
                                reftitle = reference.find("md:title", self.ns).text
                            self.relations.append({"id": refid, "title": reftitle, "uri": refURI, "type": reftype})
                        if xml.find("./md:citation/md:supplementTo", self.ns) is not None:
                            suppl = xml.find("./md:citation/md:supplementTo", self.ns)
                            suppURI = None
                            supptitle = None
                            suppyear = None
                            suppid = suppl.get("id")
                            if suppl.find("md:year", self.ns) is not None:
                                suppyear = suppl.find("md:year", self.ns).text
                            if suppl.find("md:title", self.ns) is not None:
                                supptitle = suppl.find("md:title", self.ns).text
                            if suppl.find("md:URI", self.ns) is not None:
                                suppURI = suppl.find("md:URI", self.ns).text
                            self.supplement_to = {"id": suppid, "title": supptitle, "uri": suppURI, "year": suppyear}
                        for keyword in xml.findall("./md:keywords/md:keyword", self.ns):
                            # Only iterate over actual `md:keyword`; ignore, e.g., `md:techKeyword`.
                            self.keywords.append(keyword.text)

                        panXMLMatrixColumn = xml.findall("./md:matrixColumn", self.ns)
                        self._setParameters(panXMLMatrixColumn)
                        panXMLEvents=xml.findall("./md:event", self.ns)
                        self._setEvents(panXMLEvents)
                        self._setCitation()
                    else:
                        # self.logging.append({'ERROR': 'Dataset is deleted or of unknown status: ' + str(self.datastatus)})
                        self.log(logging.ERROR, "Dataset is deleted or of unknown status: " + str(self.datastatus))
                except requests.exceptions.HTTPError as e:
                    # self.logging.append({'ERROR': 'Failed to retrieve metadata information: '+str(e)})
                    self.log(logging.ERROR, "Failed to retrieve metadata information: " + str(e))
                except ET.ParseError as e:
                    # self.logging.append({'ERROR': 'Failed to parse metadata information: '+str(e)})
                    self.log(logging.ERROR, "Failed to parse metadata information: " + str(e))
                    # print( str(xmlText))

            else:
                # self.logging.append({'ERROR': 'Data set does not exist, 404'})
                self.log(logging.ERROR, "Data set does not exist, 404: " + str(self.id))
                self.id = None
        else:
            self.log(logging.ERROR, "No HTTP response object received for: " + str(self.id))

    def getGeometry(self):
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
                if pz == 1 or len(self.events) == 1:
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

    def getParamDict(self):
        """The method returns translates the parameter object list into a dictionary"""
        paramdict = {"shortName": [], "name": [], "unit": [], "type": [], "format": []}
        for param_key, param_value in self.params.items():
            paramdict["shortName"].append(param_key)
            paramdict["name"].append(param_value.name)
            paramdict["unit"].append(param_value.unit)
            paramdict["type"].append(param_value.type)
            paramdict["format"].append(param_value.format)
        return paramdict

    def info(self):
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

    def rename_column(self, old_name, new_name):
        if self.params.get(old_name):
            self.params[new_name] = self.params.pop(old_name)
            self.params[new_name].name = new_name
            self.params[new_name].shortName = new_name
            self.data.rename(columns={old_name: new_name}, inplace=True)
            self.qcdata.rename(columns={old_name: new_name}, inplace=True)

    def to_netcdf(self, filelocation=None, save=True, type="sdn"):
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

    def to_frictionless(self, filelocation=None, save=True):
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

    def to_dwca(self, save=True):
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
        dwca_exporter = PanDarwinCoreAchiveExporter(self)
        ret = dwca_exporter.create()
        if save:
            dwca_exporter.save()
        # print(dwca_exporter.logging)
        self.logging.extend(dwca_exporter.logging)
        return ret

    def download(self, indices: list = None, columns: list[str] = None):
        """Download binary data if available; otherwise, save dataframe as CSV.

        Downloads can be very large. Consider explicitly defining the pangaeapy cache when calling PanDataSet.

        Parameters
        ----------
        indices : list
            Row indices of the data to download (e.g. [1, 2, 6]).
        columns : list of strings
            Column names of the data to download (e.g. ["Binary", "netCDF"]).

        Returns
        -------
            List of downloaded or saved filenames
        """

        if self.data.empty:
            raise ValueError(f"Dataset has no available data to download!\n"
                             f"Check {self.doi} for more information on dataset.")

        # possible names for binary column(s) in the data table of a data set
        binary_columns = ["binary", "netcdf", "image", "video", "text", "url", "csv"]
        # case-insensitive matching of any column starting with one of the possible binary column names
        # (?!.*\() -> negative look ahead assertion to skip columns with "("
        # such columns mostly contain additional information about the binary file (e.g. Binary (Size))
        pattern = "(?i)^(" + "|".join(binary_columns) + r")(?!.*\()"
        column_names = self.data.filter(regex=pattern).columns.tolist()

        if column_names:
            self.log(logging.INFO, f"Downloading files to {self.cachedir}")
            self.columns = columns if columns else column_names
            self.data_index = indices if indices else []

            # double check input
            if not all([x in column_names for x in self.columns]):
                raise ValueError(f"Not all given columns ({self.columns}) are available!\n"
                                 f"Please select one or all of {column_names}.")
            # raise error if an index is larger than the available row numbers
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


class PanDataHarvester:
    """
    Downloads binary data from the PANGAEA tape archive.

    Parameters
    ----------
    dataset: PanDataSet
        The dataset, which initiates the PanDataHarvester


    This class bundles the download functionality of pangaeapy.
    When initiated via PanDataSet.download(), the selected files are downloaded asynchronously.
    They are stored in the local cache in their original file format and under their original name.
    The Harvester will check if the file already exists before downloading.
    To use the download functionality in a jupyter notebook include

    .. code-block:: python

        import nest_asyncio
        nest_asyncio.apply()

    at the beginning of the notebook.


    """

    def __init__(self, dataset):
        self.id = dataset.id
        self.auth_token = dataset.auth_token
        self.data = dataset.data
        self.cachedir = dataset.cachedir
        self.cachedir.mkdir(parents=True, exist_ok=True)
        self.columns = dataset.columns  # list of column names
        self.data_index = dataset.data_index
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent downloads


    def _list_available_data(self):
        """List available binary data (filenames) in the dataset.

        Returns
        -------
            List of filenames
        """
        if self.data_index:
            available_data = (self.data
                              .iloc[self.data_index][self.columns]  # select rows and columns
                              .to_numpy().flatten().tolist())  # extract values and reduce dimensions
        else:
            # If no index is supplied return all rows
            available_data = self.data[self.columns].to_numpy().flatten().tolist()
        return available_data

    async def _download_file(self, session, url, filename, max_retries=4):
        """Download a single file asynchronously, handling 503 errors."""
        filepath = Path(self.cachedir, filename)

        attempt = 0
        async with self.semaphore:
            while attempt < max_retries:
                try:
                    async with session.get(url) as response:
                        size = int(response.headers.get('content-length', 0))
                        if filepath.exists() and (filepath.stat().st_size >= size):
                            print(f"File {filename} already exists, skipping.")
                            return filepath

                        if response.status == 429:
                            wait_time = response.headers.get('retry-after', 0)
                            wait_time = int(wait_time) if wait_time != 0 else 10
                            print(f"Got response status 429 (Too many connections) while trying to download {filename}.\n"
                                  f" Retrying in {wait_time} seconds.")
                            await asyncio.sleep(wait_time)
                            attempt += 1
                            continue

                        if response.status == 503:
                            sleep = 30 * (attempt + 1)
                            print(f"{filename} is being retrieved from tape. Retrying in {sleep} seconds...")
                            await asyncio.sleep(sleep)  # Wait 2 minutes before retrying
                            attempt += 1
                            continue

                        with open(filepath, "wb") as f:
                            async for chunk in response.content.iter_any():
                                f.write(chunk)

                    print(f"Downloaded {filename} successfully!")
                    return filepath

                except aiohttp.ClientResponseError as e:
                    attempt += 1
                    if attempt == max_retries:
                        raise e
                    print(f"Error {e.status} encountered. Retrying ({attempt}/{max_retries})...")

            print(f"Exceeded maximum number of retries ({max_retries}). Cancel download.")
            return None


    async def download_files(self):
        """Download all binary files asynchronously."""
        binary_files = self._list_available_data()
        dataset_id = self.id

        async with aiohttp.ClientSession() as session:
            session.headers.update({"Authorization": f"Bearer {self.auth_token}",
                                    "User-Agent": f"pangaeapy/{CURRENT_VERSION}"})
            tasks = []
            for filename in binary_files:
                if filename.startswith("https:"):
                    url = filename
                    parsed_url = urlparse(url)
                    # extract filename from url and decode special characters
                    filename = PurePosixPath(unquote(parsed_url.path)).name
                else:
                    url = f"https://download.pangaea.de/dataset/{dataset_id}/files/{filename}"

                tasks.append(self._download_file(session, url, filename))

            results = await asyncio.gather(*tasks)
            downloaded_files = [result for result in results if result]

        return downloaded_files


    def run_download(self):
        """Start asynchronous file download for single file downloads or download
         the zip file and extract its contents if the whole data set is requested.
         This requires a valid auth_token (also called Bearer Token).

         Returns
         -------
            List of downloaded files
         """
        if (self.data_index == []) and (all(["URL" not in column for column in self.columns])):
            # User wants the whole binary data set
            # Download the data set via the ZIP link, which requires a valid auth_token
            # Data sets with a URL binary column do not have a zip download available
            downloaded_files = self.download_zip_file()
        else:
            try:
                # Check if there's a running event loop
                loop = asyncio.get_running_loop()
                # If we reach here, a loop is running (e.g. in a jupyter notebook)
                future = asyncio.ensure_future(self.download_files())
                downloaded_files = loop.run_until_complete(future)
            except RuntimeError as e:
                # probably running download inside a jupyter notebook
                if str(e) ==  "This event loop is already running":
                    print("You are probably calling download() inside a jupyter notebook.\n"
                          "Insert\nimport nest_asyncio\nnest_asyncio.apply()\n"
                          "in a notebook cell before calling download().")
                    raise

                # No running event loop, create a new one
                downloaded_files = asyncio.run(self.download_files())

        return downloaded_files


    def download_zip_file(self):
        """Download a complete binary data set via the ZIP link.
        Requires a valid auth_token (also called Bearer Token), which can be found at https://www.pangaea.de/user/.
        """
        url = f"https://download.pangaea.de/dataset/{self.id}/allfiles.zip"
        zip_path = Path(self.cachedir, "allfiles.zip")
        extract_dir = Path(self.cachedir)
        url_headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "User-Agent": f"pangaeapy/{CURRENT_VERSION}"
        }
        try:
            with requests.get(url, stream=True, headers=url_headers) as r:
                if r.status_code == 401:
                    print(
                        "401 Client Error: Unauthorized access.\n"
                        "Please provide a valid auth_token when opening the data set.\n"
                        "You can find your auth_token (Bearer Token) on your PANGAEA user page at https://www.pangaea.de/user/"
                    )
                    return []
                r.raise_for_status()

                # Stream download to disk
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            # Extract and collect filenames
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                downloaded_files = [Path(self.cachedir, f) for f in zip_file.namelist()]
                zip_file.extractall(extract_dir)

            return downloaded_files

        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}")
            return []
        except zipfile.BadZipFile as e:
            print(f"Extraction failed: {e}")
            return []
        finally:
            if zip_path.exists():
                try:
                    zip_path.unlink()
                except Exception as e:
                    print(f"Failed to delete ZIP file: {e}")
