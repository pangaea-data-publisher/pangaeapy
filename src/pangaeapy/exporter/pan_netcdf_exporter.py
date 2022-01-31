# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 14:58:00 2018

@author: Robert Huber 
"""
import json
import linecache

from netCDF4 import date2num, Dataset,stringtochar
import pandas as pd
import re
import numpy as np
import sys
import os

from pangaeapy.exporter.pan_exporter import PanExporter


class PanNetCDFExporter(PanExporter):
    style = 'sdn'
    def PrintException(self):
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    
    def setMainVariables(self):
        #self.time_units = 'hours since 2000-01-01 00:00:00.0'
        self.time_units ='days since -4713-01-01T00:00:00Z'
        try:
            self.netcdf.title=self.pandataset.title
            self.netcdf.id=self.pandataset.doi
            if self.pandataset.topotype=='time series':
                self.netcdf.featureType='timeSeries'
            elif self.pandataset.topotype=='profile series' or self.pandataset.topotype=='vertical profile':
                self.netcdf.featureType='profile'
            #nc.platform_code='DBBH'
           # nc.featureType=self.pandataset.getGeometry()
            self.netcdf.principal_investigator=self.pandataset.authors[0].fullname
            self.netcdf.date_update=self.pandataset.date+'+02:00'
        except Exception as e:
            self.logging.append({'ERROR': 'NetCDF main variables creation failed '+str(e)})
            #print('NetCDF main variables creation failed '+str(e))
                      
    def setParameterSynonyms(self, mappingfile=['mappings','pan_mappings.json']):
        self.logging.append({'INFO': 'Trying to set synonyms and standard names'})
        #print('Trying to set synonyms and standard names')
        with open(os.path.join(self.module_dir,*mappingfile), 'r') as mappingjson:
            mapping = json.load(mappingjson)
        #mapping=pd.read_csv(mappingfile,delimiter='\t',index_col='PANGAEA ID')
        for pacronym, param in self.pandataset.params.items():
            if str(param.id) in mapping:
                #print(str(param.id)+" "+mapping.get(str(param.id)).get('SDN P01 ID'))
                cf_name=mapping.get(str(param.id)).get('CF standard name')
                self.pandataset.params[pacronym].addSynonym('CF',cf_name,unit=mapping.get(str(param.id)).get('CF unit'))
                self.pandataset.params[pacronym].addSynonym('SD',mapping.get(str(param.id)).get('SDN P01 Name'),id=mapping.get(str(param.id)).get('SDN P01 ID'),uri=mapping.get(str(param.id)).get('SDN P01 URI'),unit=mapping.get(str(param.id)).get('SDN Unit Name'), unit_id=mapping.get(str(param.id)).get('SDN Unit URN') )
        
    def cleanParameterNames(self):
        tempparams ={}
        for pk, p in self.pandataset.params.items():
            new_pk = re.sub(r'[/\s]', '_',pk)
            new_pk = re.sub(r'[\[\]]', '', new_pk)
            new_pk = re.sub(r'(?![a-zA-Z0-9_]|{MUTF8})([^\x00-\x1F/\x7F-\xFF]|{MUTF8})', '_', new_pk)
            self.pandataset.params[new_pk] = self.pandataset.params.pop(pk)
        self.pandataset.data.columns = self.pandataset.data.columns.str.replace(r'[/\s]', '_', regex=True)
        self.pandataset.data.columns = self.pandataset.data.columns.str.replace(r'[\[\]]', '', regex=True)
        self.pandataset.data.columns = self.pandataset.data.columns.str.replace('(?![a-zA-Z0-9_]|{MUTF8})([^\x00-\x1F/\x7F-\xFF]|{MUTF8})', '_',regex=True)

    def setSDNQCVariable(self, ncvarName, dims):
        ncVar = self.netcdf.createVariable(ncvarName, 'b', dims, fill_value='57')
        ncVar.long_name = 'SeaDataNet quality flag'
        ncVar.flag_values = ', '.join(map(str, self.pandataset.quality_flag_replace.values()))
        ncVar.flag_meanings = ' '.join(self.pandataset.quality_flags.values())
        ncVar.sdn_conventions_urn = 'SDN:L20::'
        ncVar.Conventions = 'SeaDataNet measurand qualifier flags'
        return ncVar

    def renameSDNDimVars(self):
        newNames= {'Date/Time':'TIME','Depth water':'DEPTH','Latitude':'LATITUDE','Longitude':'LONGITUDE'}
        for nkey, nname in newNames.items():
            self.pandataset.rename_column(nkey,nname)

    def setSDNVariablesAndValues(self, dims):
        eventCoords = self.pandataset.data.groupby(['Event']).min()
        self.cleanParameterNames()
        var_coordinates = [c for c in ['TIME','DEPTH','LATITUDE','LONGITUDE'] if c in set(self.pandataset.params.keys())]
        crsVar = self.netcdf.createVariable('crs', 'i')
        crsVar.grid_mapping_name = "latitude_longitude"
        crsVar.epsg_code = "EPSG:4326"
        crsVar.semi_major_axis = 6378137.0
        crsVar.inverse_flattening = 298.257223563
        crsVar.assignValue(0)

        for ncvarName, p in self.pandataset.params.items():
            if ncvarName not in list(self.netcdf.dimensions.keys()) and ncvarName!='Event':
                if p.type in ['gqc','qc','numeric'] or ncvarName in ['TIME']:
                    try:
                        if not self.pandataset.data[ncvarName].isnull().all():
                            if self.netcdf.featureType == 'profile':
                                #if ncvarName in ['Latitude','Longitude','Date_Time']:
                                if ncvarName in ['LATITUDE','LONGITUDE']:
                                    ncVar = self.netcdf.createVariable(ncvarName, 'f4', ['INSTANCE'])
                                    if ncvarName == 'LATITUDE':
                                        ncQCVar = self.setSDNQCVariable('POSITION_SEADATANET_QC', ['INSTANCE'])
                                        ncQCVar[:] = [9] * eventCoords['LATITUDE'].size
                                        ncVar.units = 'degrees_north'
                                        ncVar.long_name = 'Latitude'
                                        ncVar.axis = 'Y'
                                        ncVar.sdn_uom_name = 'Degrees north'
                                        ncVar.sdn_uom_urn = 'SDN:P06::DEGN'
                                        ncVar.ancillary_variables = "POSITION_SEADATANET_QC"
                                        ncVar.grid_mapping = "crs"
                                    else:
                                        ncVar.units = 'degrees_east'
                                        ncVar.long_name = 'Longitude'
                                        ncVar.axis = 'X'
                                        ncVar.sdn_uom_name = 'Degrees east'
                                        ncVar.sdn_uom_urn = 'SDN:P06::DEGE'
                                        ncVar.ancillary_variables = "POSITION_SEADATANET_QC"
                                        ncVar.grid_mapping = "crs"
                                    ncVar[:]=eventCoords[ncvarName].values
                                elif ncvarName == 'TIME':
                                    ncVar = self.netcdf.createVariable(ncvarName, 'd', ['INSTANCE'])
                                    ncVar[:]=date2num(eventCoords['TIME'].dt.to_pydatetime(),units=self.time_units,calendar='standard')
                                    ncQCVar = self.setSDNQCVariable('TIME_SEADATANET_QC', ['INSTANCE'])
                                    ncQCVar[:] = [9] * eventCoords['TIME'].size
                                    ncVar.sdn_uom_name = 'Days'
                                    ncVar.sdn_uom_urn = 'SDN:P06::UTAA'
                                    ncVar.axis = 'T'
                                    ncVar.units = self.time_units
                                    ncVar.long_name = 'Chronological Julian Date'
                                    ncVar.ancillary_variables = "TIME_SEADATANET_QC"
                                    ncVar.calendar = 'julian'
                                else:
                                    #make sure values to fill the nc var has the same shape (dimensions) as the variable
                                    dimshape=[]
                                    for dim in dims:
                                        dimshape.append(self.netcdf.dimensions[dim].size)
                                    if ncvarName.endswith('_SEADATANET_QC'):
                                        ncVar = self.setSDNQCVariable(ncvarName, dims)
                                    else:
                                        ncVar=self.netcdf.createVariable(ncvarName,'f4',dims)
                                        ncVar.coordinates = ' '.join(var_coordinates)

                                    if ncvarName == 'DEPTH':
                                        ncVar.axis = 'Z'
                                        ncVar.positive = 'down'

                                    if ncvarName+'_SEADATANET_QC' in self.pandataset.data.columns:
                                        ncVar.ancillary_variables =ncvarName+'_SEADATANET_QC'
                                    ncValues = np.reshape(self.pandataset.data[ncvarName].values, dimshape)
                                    ncVar[:] = ncValues
                            elif self.netcdf.featureType=='timeSeries':
                                ncVar=self.netcdf.createVariable(ncvarName,'f4',dims)
                                if ncvarName=='TIME':
                                    ncVar[:]=date2num(self.pandataset.data[ncvarName].dt.to_pydatetime(),units=self.time_units,calendar='standard')
                                else:
                                    ncVar[:]=self.pandataset.data[ncvarName].values
                            else:
                                self.logging.append({'ERROR':'NetCDF Feature type not supported'})
                                break   

                            #Setting the units                                    
                            if p.synonym['CF'] != None:
                                if p.synonym['CF'].get('name'):
                                    ncVar.standard_name=p.synonym['CF'].get('name')
                                    if isinstance(p.synonym['CF']['unit'], str) ==True and not hasattr(ncVar, 'units'):
                                        p.unit=p.synonym['CF']['unit']
                            if p.synonym['SD'] != None:
                                ncVar.sdn_parameter_name=p.synonym['SD']['name']
                                ncVar.sdn_parameter_urn=p.synonym['SD']['id']
                            if not hasattr(ncVar,'long_name'):
                                ncVar.long_name=p.name
                            if not hasattr(ncVar, 'units'):
                                if p.unit!=None:
                                    ncVar.units=p.unit
                                    if p.synonym.get('SD'):
                                        if  p.synonym['SD'].get('unit'):
                                            ncVar.sdn_uom_name = p.synonym['SD']['unit']
                                            ncVar.sdn_uom_urn = p.synonym['SD']['unit_id']
                                else:
                                    ncVar.units='1'
                                    ncVar.sdn_uom_name = 'Dimensionless'
                                    ncVar.sdn_uom_urn = 'SDN:P06::UUUU'

                    except Exception as e:
                        self.logging.append({'ERROR': 'NetCDF Variable creation failed for Param: ' + ncvarName + ', ERROR: ' + str(e)})
                        self.PrintException()
                        continue

    def create(self, style='pan'):
        self.style = style
        ret = None
        if isinstance(self.pandataset.data, pd.DataFrame):
            self.cleanParameterNames()
            self.setParameterSynonyms()
            if 'Event' in self.pandataset.data.columns:                
                #TODO: Check if data set is in water: depth water or negative elevation of event.                
                if self.pandataset.topotype=='time series' or self.pandataset.topotype=='profile series' or self.pandataset.topotype=='vertical profile':
                    if style=='pan':
                        self.createPANNetCDF()
                    elif style=='sdn':
                        self.createSDNNetCDF()                        
                else:
                    self.logging.append({'ERROR': 'NetCDF Variable creation failed: Invalid Topotype (has to be profile, timeseries or series of profiles) but is: '+str(self.pandataset.topotype)})
            else:
                self.logging.append({'ERROR': 'NetCDF Variable creation failed: Event column is missing'})
            ret = self.file
        return ret

    def save(self):
        if isinstance(self.file, memoryview):
            try:
                with open(os.path.join(self.filelocation,str('netcdf_'+str(self.style)+'_'+str(self.pandataset.id)+'.nc')),'wb') as f:
                    #print(f.name)
                    f.write(self.file)
                    f.close()
                    self.logging.append({'SUCCESS': 'Saved NetCDF at: ' + str(os.path.join(self.filelocation,str('netcdf_pangaea_'+str(self.pandataset.id)+'.nc')))})
                    return True
            except Exception as e:
                self.logging.append({'ERROR': 'Could not save, NetCDF: '+str(e)})
        else:
            self.logging.append({'ERROR':'Could not save, NetCDF file is not a memoryview-object'})
            return False
        #print(type(self.file))

    def __str__(self):
        if isinstance(self.netcdf, Dataset):
            try:
                return self.netcdf.tocdl()
            except Exception as e:
                self.logging.append({'ERROR': 'Could not convert NetCDF to CDL: '+str(e)})
                return ''
                
    def createSDNNetCDF(self):
        #print('SDN2');
        self.logging.append({'INFO':'Trying to create a SeaDataNet NetCDF file'})
        self.netcdf = None
        #Determine the maximum number od data rows per event -> SDN's MAXZ, MAXT
        maxr=self.pandataset.data.groupby('Event').size().max()
        try:
            #nc = Dataset(self.filelocation+'\\nc'+str(self.pandataset.id)+'_sdn.nc','w',format='NETCDF3_CLASSIC')
            self.netcdf = Dataset(str(self.pandataset.id)+'_sdn.nc',mode = 'w', memory=1028,format='NETCDF3_CLASSIC')
            self.netcdf.Conventions='SeaDataNet_1.0 CF-1.6'
            evfr=self.pandataset.getEventsAsFrame()
            MaxStrLen=dict()
            MaxStrLen['label']=evfr['label'].str.len().max()
            if evfr['campaign'].str.len().max() >0 :               
                MaxStrLen['campaign']=evfr['campaign'].str.len().max()
            cdi_id='pan_'+str(self.pandataset.id)
            MaxStrLen['cdi_id']=len(cdi_id)
            #CampaignStrLen=evfr['campaign'].str.len().max()
            #BasisStrLen=evfr['basis'].str.len().max()
            evcnt=len(evfr['label'])
            maxtype='MAXZ'
            self.setMainVariables()
            if self.pandataset.topotype=='vertical profile' or self.pandataset.topotype=='profile series':
                maxtype='MAXZ'
            if self.pandataset.topotype=='time series':
                maxtype='MAXT'
            self.netcdf.createDimension('INSTANCE',evcnt)
            self.netcdf.createDimension(maxtype,maxr)
            for sk, StrDimLen in MaxStrLen.items():
                if 'STRING'+str(StrDimLen) not in self.netcdf.dimensions:
                    self.netcdf.createDimension('STRING'+str(StrDimLen), StrDimLen)
            #print(np.array(evfr['label']))
            events=self.netcdf.createVariable('SDN_STATION', 'S1',(u'INSTANCE',u'STRING'+str(MaxStrLen['label'])))
            events.long_name='Event label'
            if 'campaign' in MaxStrLen:
                campaign=str(MaxStrLen['campaign'])
                campaigns=self.netcdf.createVariable('SDN_CRUISE', 'S1',(u'INSTANCE',u'STRING'+campaign))
                campaigns.long_name='Campaign label'
            #Add empty rows to have the same number of rows per event
            self.pandataset.data=pd.concat([
                d.reset_index(drop=True).reindex(range(maxr))
                for n, d in self.pandataset.data.groupby('Event')
                ], ignore_index=True)
            events[:]=stringtochar(np.array(evfr['label'].tolist(),'S'+str(MaxStrLen['label'])))
            if 'campaign' in MaxStrLen:
                campaigns[:]=stringtochar(np.array(evfr['campaign'].tolist(),'S'+str(MaxStrLen['campaign'])))
            
            #mandatory bottom depth
            sdn_depth=self.netcdf.createVariable('SDN_BOT_DEPTH','f4',['INSTANCE'], fill_value=-999)
            sdn_depth.standard_name ="sea_floor_depth_below_sea_surface"
            sdn_depth.units='meters'
            sdn_depth.long_name = "Bathymetric depth at "+self.netcdf.featureType+" measurement site"
            sdn_depth.sdn_parameter_urn = "SDN:P01::MBANZZZZ"
            sdn_depth.sdn_parameter_name = "Sea-floor depth (below instantaneous sea level) {bathymetric depth} in the water body"
            sdn_depth.sdn_uom_urn = "SDN:P06::ULAA"
            sdn_depth.sdn_uom_name = "Metres"
            # ONLY IN MARINE ENVIRONMENT !!!!
            evfr['elevation']=evfr['elevation']*-1
            evfr['elevation'].fillna(value=-999, inplace=True)
            sdn_depth[:]=evfr['elevation'].values
            #pangaeas EDMO code            
            sdn_edmo_code=self.netcdf.createVariable('SDN_EDMO_CODE','int',['INSTANCE'])
            sdn_edmo_code.long_name='European Directory of Marine Organisations code for the CDI partner'
            sdn_edmo_code[:]=[3234]*evcnt
            #the mandatory cdi id
            sdn_cdi_id=self.netcdf.createVariable('SDN_LOCAL_CDI_ID','S1',(u'INSTANCE',u'STRING'+str(MaxStrLen['cdi_id'])))
            if self.pandataset.topotype=='vertical profile' or self.pandataset.topotype=='profile series':
                sdn_cdi_id.cf_role='profile_id'
            if self.pandataset.topotype=='time series':
                 sdn_cdi_id.cf_role='timeseries_id'
            sdn_cdi_id.long_name='SeaDataNet CDI identifier'
            cdi_ids=[cdi_id]*evcnt
            sdn_cdi_id[:]=stringtochar(np.array(cdi_ids,'S'+str(MaxStrLen['cdi_id'])))
            
            self.setSDNVariablesAndValues(['INSTANCE',maxtype])
            self.file = self.netcdf.close()

            self.logging.append({'SUCCESS':'NetCDF creation successfully finished'})
        except Exception as e:                       
            if self.netcdf is not None:
                self.netcdf.close()
            self.logging.append({'ERROR':'NetCDF creation failed'+str(e)})
            self.PrintException()

    
        
    def createPANNetCDF(self):
        dim=dict()
        nc = None
        try:
            nc = Dataset(self.filelocation+'\\nc'+str(self.pandataset.id)+'_pan.nc','w',diskless=True, format='NETCDF4')
            self.cleanParameterNames()
            self.setMainVariables(nc)
            ##### PANGAEA STYLE ######
            eventGroup= self.pandataset.data.groupby('Event')
            for eventName, eventFrame in eventGroup:
                self.logging.append({'INFO':'Trying to create NetCDF Dimensions and Variables for Event: '+str(eventName)})
                eventFrame.columns = eventFrame.columns.str.replace('/','_')    
                eventFrame.columns = eventFrame.columns.str.replace(r'[\[\]]','', regex=True)
                topotypeOK=False
                depthColumn='Depth_water'
                depthDir='positive'
                datLen=1
                depLen=1
                #latNo=len(eventFrame['Latitude'].unique())
                #lonNo=len(eventFrame['Longitude'].unique())
                if 'Depth_water' in eventFrame.columns:
                    depNo=len(eventFrame[depthColumn].unique())
                else:
                    depNo=0
                if 'Elevation' in eventFrame.columns:
                    elNo=len(eventFrame['Elevation'].unique())
                else:
                    elNo=0
                if depNo >= 1:
                    depLen = len(eventFrame[depthColumn])
                #datNo=len(eventFrame['Date_Time'].unique())
                #checking topotype
                #replace with panDataSet getGeometry
                pangeotype = self.pandataset.getGeometry()
                if pangeotype in ['trajectoryProfile','timeSeries','profile']:
                    try:                               
                        ng=nc.createGroup(eventName.replace('/','-'))                        
                        ng.createDimension('Latitude', 1)
                        ncLatVar=ng.createVariable('Latitude', 'f4','Latitude')
                        ncLatVar[:]=eventFrame['Latitude'].unique()
                        ng.createDimension('Longitude', 1)
                        ncLonVar=ng.createVariable('Longitude', 'f4','Longitude')
                        ncLonVar[:]=eventFrame['Longitude'].unique()
                        ng.createDimension(depthColumn, depLen)
                        ncDepVar=ng.createVariable(depthColumn, 'f4',depthColumn)
                        if self.pandataset.topotype=='time series':
                            ncDepVar[:]=eventFrame[depthColumn].unique()
                        else:
                            ncDepVar[:]=eventFrame[depthColumn].values
                        ng.createDimension('Date_Time', datLen)
                        ncDatVar=ng.createVariable('Date_Time', 'f8','Date_Time')
                        ncDatVar.long_name='time'
                        ncDatVar.units=self.time_units
                        if self.pandataset.topotype=='time series':
                            ncDatVar[:]=date2num(eventFrame['Date_Time'].dt.to_pydatetime(),units=self.time_units,calendar='standard') 
                        else:
                            ncDatVar[:]=date2num(eventFrame['Date_Time'].iloc[0].to_pydatetime(),units=self.time_units,calendar='standard') 
                    except Exception as e:
                        self.logging.append({'ERROR':'NetCDF grouping failed '+str(e)})
                        self.PrintException()
                        break
                    
                    for pk, p in self.pandataset.params.items():
                        ncvarName=pk.replace('/','_')
                        ncvarName=re.sub('(?![a-zA-Z0-9_]|{MUTF8})([^\x00-\x1F/\x7F-\xFF]|{MUTF8})','_',ncvarName)
                        #ncvarName=re.sub(r'[\[\]]','',ncvarName)
                        if ncvarName not in list(ng.dimensions.keys()) and ncvarName!='Event':
                            if p.type=='numeric':
                                try:
                                    if not eventFrame[ncvarName].isnull().all():
                                        ncVar=ng.createVariable(ncvarName,'f4',['Latitude','Longitude',depthColumn,'Date_Time'])
                                        if p.synonym['CF']!=None:
                                            ncVar.standard_name=p.synonym['CF']
                                        ncVar.long_name=p.name
                                        if p.unit!=None:
                                            ncVar.units=p.unit
                                        else:
                                            ncVar.units='1'
                                        ncVar[:]=eventFrame[ncvarName].values
                                except Exception as e:
                                    self.logging.append({'ERROR':'NetCDF Variable creation failed for Event: '+eventName+' Param: '+ncvarName+', ERROR: '+str(e)})
                                    #print(eventFrame[ncvarName])
                                    self.PrintException()
                                    break
                else:
                    self.logging.append({'ERROR':'NetCDF Variable creation failed: Topotype could not be verified, Dataset was probably wrongly labelled as '+str(self.pandataset.topotype)})
            self.file = nc.close()
            self.logging.append({'SUCCESS': 'PAN style NetCDF created'})
        except Exception as e:                       
            if nc is not None:
                nc.close()   
            self.logging.append({'ERROR':'NetCDF creation failed'+str(e)})

