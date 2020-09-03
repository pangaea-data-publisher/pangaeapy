[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4013941.svg)](https://doi.org/10.5281/zenodo.4013941)

# pangaeapy - a Python module to access and analyse PANGAEA data

## Background

![pangaea](https://pangaea.de/assets/v.0163d8ce3a8d13294b065fcbdc04aebc/layout-images/pangaea-logo.png) 

PANGAEA (https://www.pangaea.de) is one of the world's largest archives of this kind offering essential data services such as data curation, long-term data archiving and data publication. PANGAEA hosts about 400,000 datasets comprising around 17.5 billion individual measurements (Aug. 2020) and observations which have been collected during more than 240 international research projects. The system is open to any project, institution or individual scientist using, archiving or publishing research data.

Since the programming languages Python and R have become increasingly important for scientific data analysis in recent years, we have developed 'pangaeapy'  a new, custom Python module that considerably simplifies typical data science tasks. 

Given a DOI, pangaeapy uses PANGAEA’s web services to automatically load PANGAEA metadata into a dedicated python object and tabular data into a Python Data Analysis Library (pandas) DataFrame with a mere call of a specialized function. This makes it possible to integrate PANGAEA data with data from a large number of sources and formats (Excel, NetCDF, etc.) and to carry out data analyses within a suitable computational environment such as Jupyter notebooks in a uniform manner.

## Installation

* Source code from [github](https://github.com/pangaea-data-publisher/pangaeapy)
    * `pip install git+https://github.com/pangaea-data-publisher/pangaeapy`
* Wheels for Python from [PyPI](https://pypi.org/project/pangaeapy/)
    * `pip install pangaeapy`



## Usage
```python
import pangaeapy.pandataset as pd
ds= pd.PanDataSet(787140)
print(ds.title)
print(ds.data.head())
```

## Examples
Please take a look at the example Jupyter Notebooks which you can find in the 'examples' folder

## Documentation

https://github.com/pangaea-data-publisher/pangaeapy/blob/master/docs/pandataset.md

## Cite as
Robert Huber, Egor Gordeev, Markus Stocker, Aarthi Balamurugan, & Uwe Schindler. (2020, September 3). pangaeapy - a Python module to access and analyse PANGAEA data (Version 1.0.0-beta). Zenodo. http://doi.org/10.5281/zenodo.4013941

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4013941.svg)](https://doi.org/10.5281/zenodo.4013941)

