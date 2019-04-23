# pangaeapy

This module allows to download and analyse metadata as well as data from tabular PANGAEA (https://www.pangaea.de) datasets.

## Installation

* Source code from [github](https://github.com/pangaea-data-publisher/pangaeapy)
    * `pip install git+https://github.com/pangaea-data-publisher/pangaeapy`


## Usage
```python
import pangaeapy.pandataset as pd
ds= pd.PanDataSet(787140)
print(ds.title)
print(ds.data.head())
```

## Examples
Please take a look at the example Jupyter Notebooks which you can find in the 'examples' folder
