from pangaeapy.exporter.pan_exporter import PanExporter
import os
import json
class PanPanImportExporter(PanExporter):

    def create_data_string(self):
        return self.pandataset.data.loc[:, ~self.pandataset.data.columns.isin(self.ignorecolumns)].head()

    def create_json_header(self):
        panmetadict = {}
        panmetadict["Title"] =  self.pandataset.title
        panmetadict["DataSetID"] = int(self.pandataset.id)
        panmetadict["Authors"] = []
        panmetadict["Parameter"] = []
        events = []
        eventmethodid = None
        for event in self.pandataset.events:
            events.append({'id':int(event.id),'device':int(event.deviceid)})
        if len(events) == 1:
            panmetadict["EventID"] = events[0]['id']
            eventmethodid = events[0]['device']
        for author in self.pandataset.authors:
            inst1, inst2 = 0,0
            try:
                inst1 = int(author.affiliations[0])
            except:
                pass
            try:
                inst2 = int(author.affiliations[1])
            except:
                pass
            panmetadict["Authors"].append({'ID':author.id, 'InstitutionID':inst1, 'Institution2ID':inst2})
        self.ignorecolumns = []
        for paramk, param in self.pandataset.params.items():
            if param.source not in ['event'] or not param.id:
                if not param.id and param.name=='Event label':
                    param.id = 500000
                paramdict = {'ID':param.id}
                print('Param ID', param.id)

                if param.format:
                    paramdict['Format'] = param.format
                else:
                    paramdict['Format'] = ''
                if param.methodid:
                    methodid = param.methodid
                else:
                    methodid = eventmethodid
                if methodid:
                    paramdict['MethodID'] = methodid
                if param.PI:
                    paramdict['PI_ID'] = param.PI.get('id')
                panmetadict["Parameter"].append(paramdict)
            else:
                self.ignorecolumns.append(paramk)
        print(self.ignorecolumns)
        return json.dumps(panmetadict)

    def create(self):
        panimportstr = self.create_json_header()
        print(self.create_data_string())
        return panimportstr
