from setuptools import setup

setup(
    name='pangaeapy',
    version='0.0.5',
    install_requires=['numpy','pandas','matplotlib','requests','netcdf4'],
    packages=['pangaeapy', 'pangaeapy.src', 'pangaeapy.src.exporter','pangaeapy.src.export', 'pangaeapy.src.mappings'],
    url='https://github.com/pangaea-data-publisher/pangaeapy',
    license='OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
    author='Robert Huber',
    author_email='rhuber@uni-bremen.de',
    description='This module allows to download and analyse metadata as         well as data from tabular PANGAEA (https://www.pangaea.de) datasets.                  Usage:         import pangaeapy.pandataset as pd         ds= pd.PanDataSet(787140)         print(ds.title)         print(ds.data.head())                  Please visit the github project page to see more documentation and some examples:         https://github.com/pangaea-data-publisher/pangaeapy'
)