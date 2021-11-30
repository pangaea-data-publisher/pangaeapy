import shutil

from pangaeapy.src.exporter.pan_exporter import PanExporter
import os
import json

class PanFrictionlessExporter(PanExporter):
    def create_csv(self):
        print(self.pandataset.data.head())
        csv = self.pandataset.data.to_csv(os.path.join(self.filelocation,+'data.csv'), index=False)
        print(csv)

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

    def create_package_json(self):
        panauthors = []
        for author in self.pandataset.authors:
            panauthors.append({'title': author.firstname + ' ' + author.lastname, 'role': 'author'})
        table_schema = self.create_tableschema_json()
        package = {'profile':'tabular-data-package'}
        resources=  [{'profile':'tabular-data-resource',
                      'path': 'data.csv',
                      #'format': 'csv',
                      #'mediatype': 'text/csv',
                      #'encoding': 'utf-8',
                      'schema':table_schema}]

        package['name'] = 'dataset'+str(self.pandataset.id)
        package['id'] = self.pandataset.doi
        package['title'] = self.pandataset.title
        if self.pandataset.abstract:
            package['description'] = self.pandataset.abstract
        package['created'] = self.pandataset.date
        package['contributors'] = panauthors
        package['licenses'] = [{'path':self.pandataset.licence.URI, 'name':self.pandataset.licence.label, 'title':self.pandataset.licence.name}]
        package['resources'] =resources


        with open(os.path.join(self.filelocation,'package.json'), 'w') as fp:
            json.dump(package,fp)
        print(package)

    def create(self):
        ret = False
        if not self.pandataset.isParent:
            if self.pandataset.loginstatus == 'unrestricted':
                frictionless_folder = os.path.join(self.filelocation,'frictionless'+str(self.pandataset.id))
                if os.path.exists(frictionless_folder):
                    shutil.rmtree(frictionless_folder)
                os.makedirs(frictionless_folder)
                self.create_package_json(frictionless_folder)
                self.create_csv(frictionless_folder)
                ret=frictionless_folder
                print(ret)
                print('Frictionless Exporter Created directory: '+frictionless_folder)
            else:
                print('Cannot export a protected dataset to frictionless')
        else:
            print('Cannot export a parent type dataset to frictionless')
        return ret