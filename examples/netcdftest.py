from pangaeapy.pandataset import PanDataSet

ds = PanDataSet(194695, addQC=True, QCsuffix='_SEADATANET_QC',enable_cache=False)
print(ds.data.columns)
#ds.info()
ds.to_netcdf(type='sdn')