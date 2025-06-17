import pangaeapy

project = 'pangaeapy'
copyright = '2025, Robert Huber'
author = 'Robert Huber, Johannes Röttenbacher'
version = pangaeapy.__version__
release = pangaeapy.__version__

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'myst_parser',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'furo'
html_static_path = ['_static']
