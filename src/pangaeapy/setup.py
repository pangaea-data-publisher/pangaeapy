from setuptools import setup, find_packages

setup(
    name='pangaeapy',
    python_requires='>=3.8',
    version='1.0.17',
    install_requires=['lxml>=4.9.3','requests>=2.26.0','pandas>=2.0.3','numpy>=1.24.4','netcdf4~=1.6.3', 'cftime>=1.6.3'],
    packages=['pangaeapy','pangaeapy.exporter'],
    package_dir={'pangaeapy': ''},
    #package_data={'mypkg': ['data/*.dat']},
    package_data={'pangaeapy': ['mappings/*.json','data/*.*','export/*.*']},
    include_package_data=True,
    url='https://github.com/pangaea-data-publisher/pangaeapy',
    license='OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
    author='Robert Huber',
    author_email='rhuber@uni-bremen.de',
    description='This module allows to download and analyse metadata as well as data from tabular PANGAEA (https://www.pangaea.de) datasets.                  Usage:         import pangaeapy.pandataset as pd         ds= pd.PanDataSet(787140)         print(ds.title)         print(ds.data.head())                  Please visit the github project page to see more documentation and some examples:         https://github.com/pangaea-data-publisher/pangaeapy'
)
