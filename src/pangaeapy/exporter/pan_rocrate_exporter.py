import os
from io import BytesIO
from zipfile import ZipFile

import requests
from pangaeapy.exporter.pan_exporter import PanExporter

import jmespath
import json


DATASET_JMESPATH = '''{
    "@id" : './',
    "@type" : 'Dataset',
    "name" : "name",
    "datePublished" : "datePublished",
    "description" : "description",
    "keywords" : "keywords",
    "identifier" : "identifier",
    "cite-as" : "identifier",
    "author" : "creator",
    "publisher": "publisher",
    "license" : {"@id" : "license"},
    "conditionsOfAccess" : "conditionsOfAccess",
    "isAccessibleForFree" : "isAccessibleForFree",
    "inLanguage" : "inLanguage",
    "temporalCoverage" : "temporalCoverage",
    "spatialCoverage" : "spatialCoverage",
    "variableMeasured" : "variableMeasured",
    "isPartOf" : "isPartOf",
    "hasPart": "distribution"[?contains(keys(@), '@id')].{"@type":'File',"@id":join('',['./data/',"@id"]),"contentUrl":"contentUrl", "encodingFormat":"encodingFormat"}
    }'''
CONFORMSTO = {
    'application/vnd.pangaea.metadata+xml':{'conformsto':'https://ws.pangaea.de/schemas/pansimple','name':'PANGAEA Simple XML Format'},
    'application/ld+json': {'conformsto':'https://www.schema.org/', 'name':'Schema.org JSON-LD Format'},
    'application/vnd.datacite.datacite+xml':{'conformsto':'http://datacite.org/schema', 'name':'DataCite XML Format'},
    'application/vnd.iso19139.metadata+xml':{'conformsto':'http://www.opengis.net/gml', 'name':'ISO 19139 XML Format'},
    'application/vnd.nasa.dif-metadata+xml': {'conformsto':'http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/', 'name':'NASA DIF XML Format'},
    'text/x-bibliography':{'conformsto':'text/x-bibliography', 'name':'Bibliographic Citation Plain Text'},
    'application/x-research-info-systems':{'conformsto':'application/x-research-info-systems', 'name':'Research Information Systems Citation Format'},
    'application/x-bibtex':{'conformsto':'application/x-bibtex', 'name':'Bibtex Citation Format'}
}

class PanRoCrateExporter(PanExporter):

    def __init__(self, *args, **kwargs):
        super(PanRoCrateExporter, self).__init__(*args, **kwargs)
        self.formats = self.pandataset.getMetadataFormats()
        print(self.pandataset.data.to_numpy().flatten().tolist())

    def verify(self):
        return True

    def create(self):
        in_memory_zip = False
        if self.pandataset.id:
            try:
                rocratemeta = self.get_rocrate_metadata()
                if not any('ERROR' in lg for lg in self.logging):
                    in_memory_zip = BytesIO()
                    zip_file = ZipFile(in_memory_zip, 'w')
                    for file in rocratemeta['@graph'][1].get("hasPart", []):
                        if file.get("contentUrl"):
                            filecontent = requests.get(file.get("contentUrl")).content
                            zip_file.writestr(str(file.get("@id",'file'))[2:], filecontent)
                    tempsubjectoflist = []
                    #pack alternate metadata files and complete ro-crate metadata (the subjectof details)
                    for subjectof in rocratemeta['@graph'][1].get("subjectOf", []):
                        tempsubjectof = subjectof
                        metadata, metadataid = self.pandataset.getMetadata(subjectof["encodingFormat"])
                        tempsubjectof["@id"] = './metadata/'+metadataid
                        tempsubjectoflist.append(tempsubjectof)
                        zip_file.writestr('metadata/'+metadataid, metadata)
                    rocratemeta['@graph'][1]["subjectOf"] = tempsubjectoflist
                    zip_file.writestr('ro-crate-metadata.json', json.dumps(rocratemeta, indent=2))
                    print(zip_file.namelist())
                    zip_file.close()
                    in_memory_zip.seek(0)
                    self.file = in_memory_zip
                else:
                    self.logging.append({'ERROR': 'Ro-Crate Zip file creation failed due to previous errors '})
            except Exception as e:
                self.logging.append({'ERROR': 'Ro-Crate Zip file creation failed: ' + str(e)})
        else:
            self.logging.append({'ERROR': 'Not PanDataSet object available to perform the DwC-A export'})

    def save(self):
        if isinstance(self.file, BytesIO):
            try:
                with open(os.path.join(self.filelocation,str('ro_crate_pangaea_'+str(self.pandataset.id)+'.zip')),'wb') as f:
                    #print(f.name)
                    f.write(self.file.getbuffer())
                    f.close()
                    self.logging.append({'INFO': 'Saved Ro-Crate Zip: ' + os.path.join(self.filelocation,str('ro_crate_pangaea_'+str(self.pandataset.id)+'.zip'))})
                    return True
            except Exception as e:
                self.logging.append({'ERROR': 'Could not save, Ro-Crate Zip: '+str(e)})
        else:
            self.logging.append({'ERROR':'Could not save, Ro-Crate Zip file is not a BytesIO'})
            return False

    def get_rocrate_metadata(self):
        schemametadata, schemafilename = self.pandataset.getMetadata('application/ld+json')
        schemadict = json.loads(schemametadata)
        #print("SCXHEMA METADATA: ", schemameta)
        datasetdict = jmespath.search(DATASET_JMESPATH, schemadict)
        datasetdict["subjectOf"] =[]
        for format in self.formats:
            if CONFORMSTO.get(format):
                datasetdict["subjectOf"].append({
                        "@type":"CreativeWork",
                        "encodingFormat": format,
                        "conformsTo": CONFORMSTO[format].get('conformsto'),
                        # dann muss das file an der Stelle noch rein in szip
                    })
        metadata = {
            "@context": "https://w3id.org/ro/crate/1.3/context",
            "@graph": [
                {
                  "@type": "CreativeWork",
                  "@id": "ro-crate-metadata.json",
                  "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"},
                  "about": {"@id": "./"},
                  "description": "RO-Crate Metadata Descriptor"
                },
                datasetdict,
                {
                    "@id": self.pandataset.licence.URI,
                    "@type": "CreativeWork",
                    "name": self.pandataset.licence.name,
                }
            ]
        }

        return metadata
