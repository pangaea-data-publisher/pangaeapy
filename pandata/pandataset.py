# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 13:31:30 2018

@author: Robert
"""
import requests
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import re
import io
import os
import operator
import matplotlib.pyplot as plt

class PanAuthor:
    def __init__(self,lastname, firstname=None):
        self.lastname=lastname
        self.firstname=firstname
        self.fullname=self.lastname
        if firstname!=None and  firstname!='':
            self.fullname+=', '+self.firstname

class PanEvent:
    def __init__(self, label, latitude, longitude, elevation=None, datetime=None):
        self.label=label
        self.latitude=latitude
        self.longitude=longitude
        self.elevation=elevation
        # -- NEED TO CARE ABOUT datetime2!!!
        self.datetime=datetime
        
class PanParam:
    def __init__(self, id, name, shortName, param_type,source, unit=None):
        self.id=id
        self.name=name
        self.shortName=shortName
        #self.standardName=cfName
        # Synonym dict predefined keys are CF: CF variables (), OS:OceanSites abbreviations (TEMP, PSAL etc..)
        ns=('CF','OS')
        self.synonym=dict.fromkeys(ns)
        self.type=param_type
        self.source=source
        self.unit=unit
    #def setSynonym(name, ns):
    #    self.synonym[ns]=name
    
class PanDataSet:
    def __init__(self, id):
        if type(id) is str and id.startswith('10.1594/PANGAEA'):
            self.id = id[16:]
        else:
            self.id = id
        self.ns= {'md':'http://www.pangaea.de/MetaData'}        
        # Mapping should be moved to e.g netCDF class/module??
        #moddir = os.path.dirname(os.path.abspath(__file__))
        #self.CFmapping=pd.read_csv(moddir+'\\PANGAEA_CF_mapping.txt',delimiter='\t',index_col='ID')
        self.uri='' #the doi
        self.isParent=False
        self.params=[]
        self.events=[]
        #allowed geocodes for netcdf generation which are used as xarray dimensions
        self.geocodes={1599:'Date_Time',1600:'Latitude',1601:'Longitude',1619:'Depth water'}
        self.data =pd.DataFrame()
        self.title=None  
        self.year=None
        self.authors=[]
        self.error=None
        self.loginstatus='unrestricted';
        self.allowNetCDF=True        
        self.eventInMatrix=False
        self._setMetadata()
        
        if self.loginstatus=='unrestricted' and self.isParent!=True:
            self._setData()
        else:
            print('PROBLEM: '+self.error)
            
    def getID(self,panparidstr):
        panparidstr=panparidstr[panparidstr.rfind('.')+1:]
        panparId=re.match(r"([a-z]+)([0-9]+)",panparidstr)
        pty= pid=None
        if panparId:
            return panparId.group(2)
        else:
            return False
    
    def _setEvents(self, panXMLEvents):
        for event in panXMLEvents:
            eventElevation= None
            if event.find('md:elevation',self.ns)!=None:                
                eventElevation=event.find('md:elevation',self.ns).text
            eventDateTime=None
            if event.find('md:dateTime',self.ns)!=None:
                eventDateTime= event.find('md:dateTime',self.ns).text
            self.events.append(PanEvent(event.find('md:label',self.ns).text, 
                                        event.find('md:latitude',self.ns).text, 
                                        event.find('md:longitude',self.ns).text,
                                        eventElevation,
                                        eventDateTime
                                        ))
          
    def _setParameters(self, panXMLMatrixColumn):
        col=[]
        coln=dict()
        if panXMLMatrixColumn!=None:
            for matrix in panXMLMatrixColumn:  
                panparCFName=None
                paramstr=matrix.find("md:parameter", self.ns)
                panparID=int(self.getID(str(paramstr.get('id'))))  

                panparShortName='';
                if(paramstr.find('md:shortName',self.ns) != None):
                    panparShortName=paramstr.find('md:shortName',self.ns).text
                    #Rename duplicate column headers
                    if panparShortName in coln:
                        coln[panparShortName]+=1
                        panparShortName=panparShortName+'_'+str(coln[panparShortName])
                    else:
                        coln[panparShortName]=1
                panparType=matrix.get('type')
                panparUnit=None
                if(paramstr.find('md:unit',self.ns)!=None):
                    panparUnit=paramstr.find('md:unit',self.ns).text 
                if panparShortName=='Event':
                    self.eventInMatrix=True
                #if panparID in self.CFmapping.index:
                #    panparCFName=self.CFmapping.at[panparID,'STDNAME']
                self.params.append(PanParam(panparID,paramstr.find('md:name',self.ns).text,panparShortName,panparType,matrix.get('source'),panparUnit))           
                if panparType=='geocode':
                    try:
                        panGeocode[panparShortName]=0
                    except:
                        self.allowNetCDF=False
                        self.error='Data set contains duplicate Geocodes'
                        print(self.error)
    
    def _addEventColumns(self):
        if self.eventInMatrix==False:
            self.params.append()
        
    def _setData(self, addEventColumns=True, index=True):
        col=[]
        coln=dict()
        dim=dict()
        dataURL="https://doi.pangaea.de/10.1594/PANGAEA."+str(self.id)+"?format=textfile"
        panDataTxt= requests.get(dataURL).text
        panData = re.sub(r"/\*(.*)\*/", "", panDataTxt, 1, re.DOTALL).strip() 
        #Read in PANGAEA Data    
        self.data = pd.read_csv(io.StringIO(panData), index_col=False ,error_bad_lines=False,sep=u'\t')
        #print(self.data.head())
        #Rename duplicate column headers
        for p in self.params:
            col.append(p.shortName)          
        self.data.columns=col
        # add geocode/dimension columns from Event
        if addEventColumns==True:
            if len(self.events)==1:
                print('Adding additional GEOCODE columns')
                self.data['Latitude']=self.events[0].latitude       
                self.params.append(PanParam(1600,'Latitude','Latitude','numeric','geocode','deg','latitude'))
                self.data['Longitude']=self.events[0].longitude
                self.params.append(PanParam(1600,'Longitude','Longitude','numeric','geocode','deg','longitude'))
                self.data['Elevation']=self.events[0].elevation
                self.params.append(PanParam(8128,'Elevation','Elevation','numeric','geocode','m','elevation'))
                self.data['Event']=self.events[0].label
                if 'Date/Time' not in self.data.columns:
                    self.data['Date/Time']=self.events[0].datetime
        # --- Replace Quality Flags        
        self.data.replace(regex=r'^[\?/\*#\<\>]',value='',inplace=True)
        # --- Adjust Column Data Types
        self.data = self.data.apply(pd.to_numeric, errors='ignore')
        if 'Date/Time' in self.data.columns:
            self.data['Date/Time'] = pd.to_datetime(self.data['Date/Time'], format='%Y/%m/%dT%H:%M:%S')
        #print(col)
        
    def _setMetadata(self):
        metaDataURL="https://doi.pangaea.de/10.1594/PANGAEA."+str(self.id)+"?format=metainfo_xml"
        r=requests.get(metaDataURL)
        if r.status_code!=404:
            xmlText=r.text            
            xml = ET.fromstring(xmlText)
            self.loginstatus=xml.find('./md:technicalInfo/md:entry[@key="loginOption"]',self.ns).get('value')
            if self.loginstatus!='unrestricted':
                self.error='Data set is protected'
            hierarchyLevel=xml.find('./md:technicalInfo/md:entry[@key="hierarchyLevel"]',self.ns)
            if hierarchyLevel!=None:
                if hierarchyLevel.get('value')=='parent':
                    self.error='Data set is of type parent, please select one of its child datasets'
                    self.isParent=True
            self.title=xml.find("./md:citation/md:title", self.ns).text
            self.year=xml.find("./md:citation/md:year", self.ns).text
            self.doi=self.uri=xml.find("./md:citation/md:URI", self.ns).text
            topotypeEl=xml.find("./md:extent/md:topoType", self.ns)
            if topotypeEl!=None:
                self.topotype=topotypeEl.text
            for author in xml.findall("./md:citation/md:author", self.ns):
                self.authors.append(PanAuthor(author.find("md:lastName", self.ns).text, author.find("md:firstName", self.ns).text))
            panXMLMatrixColumn=xml.findall("./md:matrixColumn", self.ns)
            self._setParameters(panXMLMatrixColumn)
            panXMLEvents=xml.findall("./md:event", self.ns)
            self._setEvents(panXMLEvents)
        else:
            self.error='Data set does not exist'
            print(self.error)
            
        
