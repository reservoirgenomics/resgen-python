Getting Started
################


Installation
-------------

Install using pip:


.. code-block:: bash

    pip install resgen-python

Logging in
----------

For now, we recommend storing your username and password in environment variables and using them to create a ``ResgenConnection``:

.. code-block:: python

  import os
  import resgen as rg

  rgc = rg.connect(
    os.getenv('RESGEN_USERNAME'),
    os.getenv('RESGEN_PASSWORD')
  )

If the parameters to ``rg.connect`` are omitted, it will automatically try to load the username and password from the environment variables ``RESGEN_USERNAME`` and ``RESGEN_PASSSWORD``. It's often easiest to place these in a ``.env`` file. This can
then be loaded using `python-dotenv`:

.. code-block:: python

  from dotenv import load_dotenv
  import os.path as op
  load_dotenv(op.expanduser('~/.resgen/credentials'))

  import resgen as rg
  rgc = rg.connect()


Projects
--------

Once logged in, all activity needs to take place within a project. This is managed by a ``ResgenProject`` object. The ``find_or_create_project`` function checks to see if a project exists for this user and returns it if it does or creates it if it doesn't.

.. code-block:: python

  project = rgc.find_or_create_project('My project')

Projects can also be associated with groups. To retrieve a project from a group, pass in the group name as a parameter:

.. code-block:: python

  project = rgc.find_or_create_project('My project', group='Group Name')

Datasets
--------

Adding Datasets
^^^^^^^^^^^^^^^

Use ``sync_dataset`` to upload data to a project. This function will check if a dataset with this filename exists in the project and uploads the local file if it doesn't. If a dataset with an equivalent filename exists in the project, this command will simply return its uuid.

.. code-block:: python

  project.sync_dataset(
    'AdnpKO.1000.mcool',datatype="matrix", 
    sync_remote=False, filetype="cooler", assembly="mm10"
  )

If the passed in dataset is a url, then it will first be downloaded and then added to the project. This may take some
time during which the dataset will appear to be there but
actually be incomplete.

Updating metadata
^^^^^^^^^^^^^^^^^

Metadata can be passed in piecewise and only the fields that
are included will be updated:

.. code-block:: python

  import resgen

  rgc = resgen.connect()
  rgc.update_dataset('daTaSetUuiD',
                     {
                      "name": "newname",
                      "description": "newdescription",
                      "tags": [
                          {"name": "some:tag"},
                          {"name": "another:tag"}
                      ]})

Finding Data
------------

To find data, search for it using a `ResgenConnection` (find operations are
not project specific). It's often useful to place them into a dictionary for
future use:

.. code-block:: python

  datasets = dict([
    (d.name, d) for d in rgc.find_datasets(project=project, limit=20)
  ])

In the following examples, we assume that the first result is the one we're looking for. In practice, this should be verified.

Finding chromsizes
^^^^^^^^^^^^^^^^^^

.. code-block:: python

  chromsizes = rgc.find_datasets(
    datatype='chromsizes', assembly='mm9'
  )[0]

Using genomic coordinates
^^^^^^^^^^^^^^^^^^^^^^^^^

Using the ``chromsizes`` dataset found in the previous section, we can create
a ``ChromosomeInfo`` object to convert genomic locations to absolute positions
assuming all the chromosomes are concatenated.

.. code-block:: python

  >> chrominfo = rgc.get_chrominfo(chromsizes)
  >> chrominfo.to_abs('chr8', 8.67e6)
  1149815680.0

We can also use a genomic range and (optionally) pad it.

.. code-block:: python

  >> chrominfo.to_abs_range('chr1', 0, 100, padding=0.1)
  [-10.0, 110.0]

This will come in handy when we make interactive figures centered on a particular region.

Finding gene annotations
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

  gene_annotations = rgc.find_datasets(
      datatype='gene-annotations', assembly='mm9'
  )[0]

Using gene annotation coordinates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

  >> gene = rgc.get_gene(gene_annotations, 'CXCR3')
  >> chrominfo.to_gene_range(gene, padding=0.1)
  [2951868790.8, 2951871913.2]

Viewing Data
------------

To view a dataset, we typically need the dataset itself (see Managing Data above) as well as a location. Locations in genomic data typically consist of a chromosome and a position. Because HiGlass shows concatenated version of chromosomes, we need to convert genomic (chromosome, position) to "absolute" coordinates using a chromsizes file.

Creating interactive figures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Datasets can be interactively viewed using the `higlass-python <https://docs-python.higlass.io>`_ package. An example can be seen below:

.. code-block:: python

  import higlass
  from higlass.client import View

  initialXDomain = [
      chrominfo.to_abs('chr8', 8.67e6),
      chrominfo.to_abs('chr8', 14.85e6)
  ]

  view1 = View([
      ds_dict['AdnpKO.1000.mcool'].hg_track(height=300),
  ], initialXDomain=initialXDomain, x=0, width=6)
  view2 = View([
      ds_dict['WT.1000.mcool'].hg_track(height=300),
  ], initialXDomain=initialXDomain, x=6, width=6)


  display, server, viewconf = higlass.display([view1, view2])
  display

Authorization Token
^^^^^^^^^^^^^^^^^^^

To view private datasets, we need to pass an authorization header to higlass:

.. code-block:: python
  display, server, viewconf = higlass.display(
    [view1, view2],
    auth_token=f"Bearer {rgc.get_token()}"
  )


Saving Figures
--------------

Interactive figures can be saved to a project using a ``higlass-python`` - generated viewconf. Note that the figure will be re-rendered and may not look exactly like the one generated by the HiGlass Jupyter widget. For finer control over figure quality, use the resgen web interface.

.. code-block:: python

  project.sync_viewconf(viewconf, "Figure 1D")

To export the figure as SVG or PNG, use the config menu in one of the higlass view headers.

Saving a notebook
-----------------

If running in a Jupyter notebook, it can be helpful to sync the notebook itself with the resgen project. This can be done using some cell
magic. First some javascript:

.. code-block:: python

  %%javascript
  var nb = IPython.notebook;
  var kernel = IPython.notebook.kernel;
  var command = "NOTEBOOK_FULL_PATH = '" + nb.notebook_path + "'";
  kernel.execute(command);

Followed by a Python sync:

.. code-block:: python

  import os
  import os.path as op

  project.sync_dataset(op.join(os.getcwd(), NOTEBOOK_FULL_PATH), force_update=True)


[Experimental] Syncing a UCSC track hub
---------------------------------------

.. code-block:: python

  project.sync_track_hub('http://193.147.188.155/hubs/BraLan/')


