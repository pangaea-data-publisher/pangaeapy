import shutil
from io import BytesIO
from zipfile import ZipFile

from pangaeapy.exporter.pan_exporter import PanExporter
import os
import json
class PanFrictionlessExporter(PanExporter):
    def get_csv(self):
        csv =''
        #print(self.pandataset.data.head())
        try:
            csv = self.pandataset.data.to_csv(os.path.join(self.filelocation,+'data.csv'), index=False)
        except Exception as e:
            self.logging.append({'ERROR': 'Frictionless CSV creation failed: '+str(e)})

        return csv

    def create_tableschema_json(self):
        schema = {'fields':[]}
        typeconv={'numeric':'number'}
        for k, p in self.pandataset.params.items():
            field={'name':k,'title':p.name}
            #print(k, p.name, p.shortName)
            pantype = p.type
            if pantype in typeconv:
                pantype = typeconv.get(pantype)
            field['type']= pantype
            panunit = p.unit
            if panunit:
                field['unit'] = panunit
            pancomment = p.comment
            if pancomment:
                field['description'] = pancomment
            schema['fields'].append(field)
        return schema

    def get_package_json(self):
        package = {'profile':'tabular-data-package'}
        panauthors = []
        try:
            for author in self.pandataset.authors:
                panauthors.append({'title': author.firstname + ' ' + author.lastname, 'role': 'author'})
            table_schema = self.create_tableschema_json()

            resources=  [{'profile':'tabular-data-resource',
                          'path': self.pandataset.id + '_data.csv',
                          'schema':table_schema}]

            package['name'] = self.pandataset.id + '_metadata'
            package['id'] = self.pandataset.doi
            package['title'] = self.pandataset.title
            if self.pandataset.abstract:
                package['description'] = self.pandataset.abstract
            package['created'] = self.pandataset.date
            package['contributors'] = panauthors
            package['licenses'] = [{'path':self.pandataset.licence.URI, 'name':self.pandataset.licence.label, 'title':self.pandataset.licence.name}]
            package['resources'] =resources
        except Exception as e:
            self.logging.append({'ERROR': 'Frictionless JSON creation failed: '+str(e)})
        return json.dump(package)

    def create(self):
        in_memory_zip = False
        ret = False
        if not self.pandataset.isParent:
            if self.pandataset.loginstatus == 'unrestricted':
                try:
                    in_memory_zip = BytesIO()
                    zip_file = ZipFile(in_memory_zip, 'w')
                    package = self.get_package_json()
                    csv = self.get_csv()
                    zip_file.writestr(self.pandataset.id + '_data.csv', csv)
                    zip_file.writestr(self.pandataset.id + '_metadata.json', package)
                    self.logging.append({'SUCCESS': 'Frictionless in memory ZIP created'})
                except Exception as e:
                    self.logging.append({'ERROR': 'Frictionless in memory Zip creation failed: '+str(e)})
            else:
                self.logging.append({'ERROR': 'Dataset is protected'})
        else:
            self.logging.append({'ERROR':'Cannot export a parent type dataset to frictionless'})
        return in_memory_zip

    def save(self):
        if isinstance(self.file, BytesIO):
            try:
                with open(os.path.join(self.filelocation,str('frictionless_pangaea_'+str(self.pandataset.id)+'.zip')),'wb') as f:
                    f.write(self.file.getbuffer())
                    f.close()
                    return True
            except Exception as e:
                self.logging.append({'ERROR': 'Could not save Frictionless Zip: '+str(e)})
        else:
            self.logging.append({'ERROR':'Could not save, Frictionless Zip file is not a BytesIO'})
            return False