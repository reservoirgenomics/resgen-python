import os.path as op
import os

import logging

logger = logging.getLogger(__name__)

def get_local_datasets(directory):
    """Get a list of all local datasets within the directory."""
    local_datasets = []

    for path, folders, files in os.walk(directory):
        if path.startswith(op.join(directory, '.resgen')):
            continue

        for folder in folders:
            full_folder = op.join(path, folder)
            if full_folder.startswith(op.join(directory, '.resgen')):
                # skip the .resgen metadata directory
                continue

            local_datasets += [{
                "fullpath": op.relpath(full_folder, directory),
                "name": folder,
                "is_folder": True,
            }]
        for file in files:
            full_file = op.join(path, file)

            local_datasets += [{
                "fullpath": op.relpath(full_file, directory),
                "name": file,
                "is_folder": False,
            }]

    # We'll go through and associate indexfiles
    by_fullpath = dict([(d['fullpath'], d) for d in local_datasets])
    to_remove = set()

    for d in local_datasets:
        for index_extension in ['bai', 'fai', 'tbi']:
            index_path = f"{d['fullpath']}.{index_extension}"
            if index_path in by_fullpath:
                d['index_filepath'] = index_path
                if index_extension in ['.bai', '.tbi']:
                    # We have no use for .bai files outside of as indexes
                    # .fai files we can add as chromsizes
                    to_remove.add(index_path)


    local_datasets = [d for d in local_datasets if d['fullpath'] not in to_remove]
    
    return local_datasets


def get_remote_datasets(project):
    """Get all remote datasets for the project.
    
    This function will consolidate paths based on containing folder ids.
    """
    datasets = project.list_datasets(limit=10000)
    ds_by_uid = dict([(ds.uuid, ds) for ds in datasets])
    ds_by_fullpath = {}

    remote_datasets = []

    for ds in datasets:
        filename = ds.name
        ds.fullname = ds.name

        ds_json = {
            "uuid": ds.uuid,
            "name": ds.name,
            "fullname": ds.name,
            'index_filepath': ds.indexfile
        }

        while ds.containing_folder:
            ds1 = ds_by_uid[ds.containing_folder]
            filename = op.join(ds1.name, filename)

            ds = ds1

        ds_json['fullname'] = filename
        remote_datasets += [ds_json]

    return dict([(ds['fullname'], ds) for ds in remote_datasets])

def remove_stale_remote_datasets(project, local_datasets, remote_datasets):
    """Remove any remote datasets which are not reflected in the local
    datasets."""
    local_fullpath = dict([(d['fullpath'], d) for d in local_datasets])

    for remote_fullpath, ds in remote_datasets.items():
        if remote_fullpath not in local_fullpath:
            logger.info("Removing stale remote dataset: %s", remote_fullpath)
            project.delete_dataset(ds['uuid'])


def add_and_update_local_datasets(project, local_datasets, remote_datasets, base_directory, link=True):
    """Add local datasets to remote if they're missing.
    
    :param base_directory: The base directory to which all local filepaths are
        relative.
    :param link: Add as links rather than uploading. Useful if running a local
        version of resgen.
    """
    def get_parent_uuid(dataset):
        parent_dir = op.split(dataset['fullpath'])[0]
        parent = remote_datasets.get(
            parent_dir
        )
        if parent:
            parent = parent['uuid']
        
        return parent
    
    for dataset in local_datasets:
        if dataset['fullpath'] in remote_datasets:
            # see if we have to update it
            rd = remote_datasets[dataset['fullpath']]
            if rd.get('index_filepath') != dataset.get('index_filepath'):
                logger.info("Updating indexfile for %s with %s", dataset['fullpath'], dataset.get('indexfile'))
                project.conn.update_dataset(rd['uuid'], {
                    "indexfile": dataset.get('index_filepath')
                })
        else:
            if dataset['is_folder']:
                parent = get_parent_uuid(dataset)
                uuid = project.add_folder_dataset(
                    folder_name=dataset['name'],
                    parent=parent
                )
                remote_datasets[dataset['fullpath']] = {
                    'uuid': uuid,
                    "is_folder": True
                }
            else:
                # Handle adding a file dataset
                parent = get_parent_uuid(dataset)
                logger.info("Adding dataset name: %s datafile: %s, parent: %s", dataset['name'], dataset['fullpath'], parent)
                if link:
                    uuid = project.add_link_dataset(
                        filepath=dataset['fullpath'],
                        index_filepath=dataset.get('index_filepath'),
                        name=dataset['name'],
                        parent=parent,
                        private=False
                    )
                else:
                    uuid = project.add_upload_dataset(
                        filepath=op.join(base_directory, dataset['fullpath']),
                        index_filepath=dataset.get('index_filepath') and op.join(base_directory, dataset.get('index_filepath')),
                        name=dataset['name'],
                        parent=parent,
                        private=False
                    )

                logger.info("Added with uuid: %s", uuid)
                remote_datasets[dataset['fullpath']] = {
                    'uuid': uuid,
                    "is_folder": False
                }

