from pangaeapy.exporter.pan_exporter import PanExporter
import os
import json
class PanPanImportExporter(PanExporter):

    def create_json_header(self):
        panmetadict = {}
        panmetadict["Title"] =  self.pandataset.title
        panmetadict["DataSetID"] = int(self.pandataset.id)
        panmetadict["Authors"] = []
        panmetadict["Parameter"] = []
        events = []
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

        for param in self.pandataset.params.values():
            if param.methodid:
                methodid = param.methodid
            else:
                methodid = eventmethodid
            panmetadict["Parameter"].append({'ID':param.id,'Format':param.format,"PI_ID":int(param.PI.get('id')),'MethodID':methodid})
        return json.dumps(panmetadict)

    def create(self):
        panimportstr = self.create_json_header()
        return panimportstr
