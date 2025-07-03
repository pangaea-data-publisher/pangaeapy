How To
======

We always assume you have imported `pangaeapy` as

.. code-block:: python

    import pangaeapy as pan

Download a specific file from a binary data set
-----------------------------------------------

Got to the landing page of the data set (e.g. https://doi.pangaea.de/10.1594/PANGAEA.956151), click on the "View dataset as HTML" button and get the row index (starting from 0) and the column name.

.. code-block:: python

    ds = pan.PanDataSet(956151, enable_cache=True, cachedir='/your/cache/path')
    filenames = ds.download(indices=[3], columns=["Binary"])

Download all files from a binary data set
-------------------------------------------

Create a user account at `PANGAEA <https://www.pangaea.de/user/signup.php>`_ and copy your bearer token from your `user page <https://www.pangaea.de/user/>`_.

.. code-block:: python

    ds = pan.PanDataSet(956151, enable_cache=True,
                        cachedir='/your/cache/path',
                        auth_token='your_personal_bearer_token')
    filenames = ds.download()

.. note::

    For tabular data sets no bearer token is required.

Set a custom cache directory
----------------------------

.. code-block:: python

    import pangaeapy as pan
    ds = pan.PanDataSet(956151, enable_cache=True,
                        cachedir='/path/to/your/storage')

