User Guide
==========

*pangaeapy*'s main purpose is to automate data retrieval and make your research reproducible for others.
To achieve this there are two main classes ``PanQuery`` and ``PanDataSet``.
With ``PanQuery`` you can make sure others find the same data as you do and with ``PanDataSet`` others can access the same data as you have.

For tutorials on using ``PanQuery`` please have a look at the `community workshop material <https://github.com/pangaea-data-publisher/community-workshop-material>`_.

Working with data sets from PANGAEA
-----------------------------------

A common scientific workflow is to search for data directly on `PANGAEA`_ and then download the data set(s) to local storage and continue work from there.

*pangaeapy* helps you with the second step by making the data available via a programmatic interface. So no need to download every data set by hand.

PANGAEA hosts basically two types of data sets: **tabular** and **binary** data. There are also collections or bibliographies, which bundle data sets together and do not actually contain data themselves. However, for the above mentioned workflow only the tabular and binary data sets are of importance.

Tabular data
^^^^^^^^^^^^

Once you found the data set you need on `PANGAEA`_, you can use its ``id`` to open it with *pangaeapy*. The ``id`` are the last digits of the data set's DOI. Thus, this data set https://doi.org/10.1594/PANGAEA.900388 would have the ``id`` ``900388``.

.. code-block:: python

    import pangaeapy as pan
    ds = pan.PanDataSet(900388, enable_cache=True)

The ``enable_cache`` keyword tells *pangaeapy* to cache the data set locally, which for tabular data sets it will do using `pickle`_. The default cache location is ``~/.pangaeapy_cache/``. You can change the location of the cache by directly providing it via a keyword argument.

.. code-block:: python

    ds = pan.PanDataSet(900388, enable_cache=True,
                        cachedir='/path/to/your/storage')

You can now access all the metadata provided with the data set on PANGAEA using the ``PanDataSet`` object such as ``title``, ``authors`` (which is a list of ``PanAuthor`` objects), ``doi`` and so forth.

For tabular data sets you can access a pandas DataFrame under ``data``.

>>> ds.data.head()
   Time sec  Altitude  ...                        Event           Date/Time
0   38314.0         6  ...  P5_232_HALO_2022_2203100101 2022-03-10 10:10:04
1   38315.0         6  ...  P5_232_HALO_2022_2203100101 2022-03-10 10:10:04
2   38316.0         6  ...  P5_232_HALO_2022_2203100101 2022-03-10 10:10:04
3   38317.0         6  ...  P5_232_HALO_2022_2203100101 2022-03-10 10:10:04
4   38318.0         6  ...  P5_232_HALO_2022_2203100101 2022-03-10 10:10:04
[5 rows x 17 columns]

If you want to access the data outside of python you can download the data as a csv file to your local cache using the ``download`` method.

>>> ds.download()
Dataset saved to /path/to/your/storage/900388_data.csv
['/path/to/your/storage/900388_data.csv']

Binary data
^^^^^^^^^^^

Binary data refers to everything, which is not stored as a table in PANGAEA's database. This includes, among others, images, videos and `netCDF`_ files. If you open a binary data set you still get a table from the ``data`` attribute but this will only list the available files in the data set.

You can also view this table on `PANGAEA`_ by clicking the "View dataset as HTML" button in the *Download Data* section on the data set landing page.

To work with these files you need to download them and read them in with a specific module such as `xarray`_ for netCDF files. *pangaeapy* can only help with the first step here. However, the ``download`` method will return a list with the paths to the downloaded files, which you can reuse in your work.

.. code-block:: python

    ds = pan.PanDataSet(944070, enable_cache=True,
                        auth_token='abcdfeghijklmnopqrstuvwxyz')
    filepaths = ds.download()

Binary data sets can get quite large with respect to file size. To protect the infrastructure behind PANGAEA you are requested to provide your personal ``auth_token``, also called bearer token, when opening the data set. Only then are you able to download the complete data set.

You can find your personal bearer token in your `PANGAEA user profile`_ meaning you need to create an account with PANGAEA.

.. note::

    Your bearer token changes everytime you log out of PANGAEA. Thus, when you accidentally share your code with your bearer token you can just log out to make it invalid.

If you only need one or a couple of files from the data set you can also directly provide the row indices of these as a list to the ``download`` method. This also works without a bearer token in order to simplify sharing code or tutorials for a specific data set.

.. code-block:: python

    ds = pan.PanDataSet(956151, enable_cache=True)
    filepaths = ds.download(indices=[0, 1, 2], columns=['Binary'])

Some data sets also have multiple types of binary data such as a netCDF file and a quicklook image. For such cases you can provide a list of column names to include in your download via the ``columns`` keyword. You can find the column names available via the aforementioned "View data set as HTML" button on the landing page of the data set (e.g. https://doi.pangaea.de/10.1594/PANGAEA.956151).

.. note::

    Binary data is mostly stored in a tape archive at PANGAEA. This means requesting a single file includes getting the tape and reading it into memory. This may take a while. However, PANGAEA keeps this file in a cache for a while after the initial request. So downloading the same file again should be faster.

.. note::

    When requesting single files *pangaeapy* limits the download to five simultaneous requests. So providing more than five indices increases the download time.

.. _PANGAEA: https://www.pangaea.de/
.. _PANGAEA user profile: https://www.pangaea.de/user/
.. _netCDF: https://de.wikipedia.org/wiki/NetCDF
.. _pickle: https://docs.python.org/3/library/pickle.html
.. _xarray: https://docs.xarray.dev/en/stable/
