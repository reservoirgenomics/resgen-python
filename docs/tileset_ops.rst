Tileset operations
##################

Fetching a tileset's name
-------------------------

First, make sure you've set up your credentials as described in
`Logging in <getting_started.html#logging-in>`_. Then execute the
following to retrieve a tileset's name:

.. code-block:: python

    import os
    import resgen as rg

    rgc = rg.connect()

    dataset = rgc.get_dataset('UvVPeLHuRDiYA3qwFlm7xQ')
    print(dataset.name)