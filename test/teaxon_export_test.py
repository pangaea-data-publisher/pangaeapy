from src.pangaeapy.pandataset import PanDataSet
from src.pangaeapy.exporter import PanDarwinCoreArchiveExporter
from src.pangaeapy.exporter.pan_rocrate_exporter import PanRoCrateExporter

import json
id = 861303
id = 623664
ds = PanDataSet(id, expand_terms=True)
print(ds.title)
testtype = 'taxon'
#print(ds.data.head())
if testtype=='rocrate':
    rde = PanRoCrateExporter(ds)
    rde.create()
    rde.save()
    print(json.dumps(rde.logging, indent=2))

#print(json.dumps(schemadata, indent=2))
'''tds = PanDarwinCoreArchiveExporter(ds)
datacolumns = tds.get_taxon_columns()
data = tds.get_dwca_data(datacolumns)
#print(data)
print(tds.param_statistics)
print(list(datacolumns.keys()))'''

if testtype == 'taxon':
    tds = PanDarwinCoreArchiveExporter(ds)
    tds.verify()
    #datacolumns = tds.get_taxon_columns()
    #data = tds.get_dwca_data(datacolumns)
    print(json.dumps(tds.logging, indent=2))

    #print(data)