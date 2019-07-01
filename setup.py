import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
# Project description
descr = """
        This module allows to download and analyse metadata as
        well as data from tabular PANGAEA (https://www.pangaea.de) datasets.
        
        Usage:
        import pangaeapy.pandataset as pd
        ds= pd.PanDataSet(787140)
        print(ds.title)
        print(ds.data.head())
        
        Please visit the github project page to see more documentation and some examples:
        https://github.com/pangaea-data-publisher/pangaeapy
        """  
# Setup
setuptools.setup(
    name="pangaeapy",
    version="0.0.3",
    author="PANGAEA® - Data Pub­lisher for Earth & En­vir­on­mental Sci­ence,Robert Huber,Markus Stocker,Egor Gordeev",
    author_email="rhuber@uni-bremen.de",
    description=descr,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pangaea-data-publisher/pangaeapy",
    install_requires=['numpy','pandas','matplotlib','requests'], #packages which are not a part of Python’s standard library.Pip will install these dependancies
    packages=setuptools.find_packages(), #find_packages() to automatically discover all packages and subpackages
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering",
    ],
)
