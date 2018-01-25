# -*- coding: utf-8 -*-
import glob
import os
import sys

from ensimpl.utils import multikeysort

ENSIMPL_DBS = None
ENSIMPL_DBS_DICT = None


def get_ensimpl_db(version, species):
    """
    Get the database based upon ``version``.

    Args:
        version (int): the version number
        species (species): the species

    Returns:
        the database file

    Raises:
        ValueError if unable to find the ``version``
    """
    try:
        return ENSIMPL_DBS_DICT['{}:{}'.format(int(version), species)]
    except Exception as e:
        raise ValueError('Unable to find version "{}" and species "{}"'.format(version, species))


def get_all_ensimpl_dbs(directory):
    """
    Configure the "global" list of ensimpl db files in ``directory``.

    Args:
        directory (str): directory path
    """
    file_str = os.path.join(directory, 'ensimpl.*.db3')
    dbs = glob.glob(file_str)

    db_list = []
    db_dict = {}

    # get the versions
    for db in dbs:
        version = int(db.split('.')[-3])
        species = db.split('.')[-2]
        combined_key = '{}:{}'.format(version, species)
        val = {'version': version, 'species': species, 'db': db}
        db_list.append(val)
        db_dict[combined_key] = val

    all_sorted_dbs = multikeysort(db_list, ['-version', 'species'])

    print(all_sorted_dbs)

    global ENSIMPL_DBS
    ENSIMPL_DBS = all_sorted_dbs
    global ENSIMPL_DBS_DICT
    ENSIMPL_DBS_DICT = db_dict


def init(directory=None):
    """
    Initialize the "global" database variables.

    Args:
        directory (str): a directory that holds the data files
           if None the environment variable ENSIMPL_DIR will be used
    """
    ensimpl_dir = os.environ.get('ENSIMPL_DIR', None)

    if directory:
        ensimpl_dir = directory

    if not ensimpl_dir:
        print('ENSIMPL_DIR not configured in environment or directory '
              'was not supplied as an option')
        sys.exit()

    if not os.path.isabs(ensimpl_dir):
        ensimpl_dir = os.path.abspath(os.path.join(os.getcwd(), ensimpl_dir))
    else:
        ensimpl_dir = os.path.abspath(ensimpl_dir)

    if not os.path.exists(ensimpl_dir):
        print('Specified ENSIMPL_DIR does not exist: {}'.format(ensimpl_dir))
        sys.exit()

    if not os.path.isdir(ensimpl_dir):
        print('Specified ENSIMPL_DIR is not a directory: {}'.format(ensimpl_dir))
        sys.exit()

    get_all_ensimpl_dbs(ensimpl_dir)
