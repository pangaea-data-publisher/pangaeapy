import pandas as pd

md = pd.read_csv('../data/parameter_mapping.csv')
md.set_index('PANGAEA ID', inplace = True)
print(md.index.dtype)
print(md.columns)
print(md.to_json('pan_mappings.json',orient='index'))