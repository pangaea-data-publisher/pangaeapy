"""
pangaeapy is a package allowing to download and analyse metadata
as well as data from tabular PANGAEA (https://www.pangaea.de) datasets.
"""

__all__ = ["exporter", "PanDataSet", "PanQuery"]

from . import exporter
from .pandataset import PanDataSet
from .panquery import PanQuery
