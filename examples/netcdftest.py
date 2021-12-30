from src import PanDataSet

ds = PanDataSet('https://doi.pangaea.de/10.1594/PANGAEA.868913', enable_cache=False)
#for k,p  in ds.params.items():
#    print(k, p.name, p.shortName)
print(ds.doi, ds.id, ds.uri)
print(ds.abstract)
if ds.licence:
    print(ds.licence.URI)
for au in ds.authors:
    print(au.id)
print('-------')
for pr in ds.authors:
    print(pr.id)
#path = ds.to_frictionless()
#print(path)

#valrep= validate_package(path+'\\package.json')
#print(valrep)
#valrep= validate(path+'\\data.csv')
#print(valrep)
#print(ds.data.columns)
#ds.info()
#ds.to_netcdf(type='sdn')