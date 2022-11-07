from src.pangaeapy.pandataset import PanDataSet
from src.pangaeapy.exporter.pan_panimport_exporter import PanPanImportExporter

dsid = '10.1594/PANGAEA.943571' #multiple events
dsid = '10.1594/PANGAEA.945447' #curation level
dsid= '10.1594/PANGAEA.774196'
ds = PanDataSet(dsid)
#print(ds.getEventsAsFrame().columns)
panimport_exporter = PanPanImportExporter(ds)
print(panimport_exporter.create())

