Command line interface
######################

Installation
-------------

Install using pip:


.. code-block:: bash

    pip install resgen-python

Logging in
----------

Either place the following lines in a file at ``~/.resgen/credentials`` or you will be prompted for a username and password:

.. code-block:: bash
  
    RESGEN_USERNAME=my_username
    RESGEN_PASSWORD=my_password

Syncing data
------------

To sync a local dataset, run the following command:

.. code-block:: bash

  resgen sync datasets user project filename --tag filetype:somefiletype

If you'd like to associate an index file with a filepath, include both filepaths separated by a comma:

  resgen sync datasets user project my_file.bam,my_file.bai --tag filetype:somefiletype

