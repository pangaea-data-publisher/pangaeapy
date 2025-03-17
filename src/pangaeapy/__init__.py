"""
pangaeapy is a package allowing to download and analyse metadata
as well as data from tabular PANGAEA (https://www.pangaea.de) datasets.
"""
from pangaeapy.pandataset import PanDataSet
from pangaeapy.panquery import PanQuery

__all__ = [
    "PanDataSet",
    "PanQuery",
]
