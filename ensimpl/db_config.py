# -*- coding: utf-8 -*-

from collections import OrderedDict
import glob
import os
import sys

ENSIMPL_DBS = None
ENSIMPL_DB_DEFAULT = None


def get_ensimpl_db(version=None):
    """
    Get the database based upon ``version``.
    :param int version: the version number
    :return: the database file
    """
    try:
        if version:
            return ENSIMPL_DBS[int(version)]

        return ENSIMPL_DB_DEFAULT
    except Exception as e:
        raise ValueError("Unable to find version: {}".format(version))


def get_all_ensimpl_dbs(directory):
    """
    Get a list of all files in ``directory``.

    :param str directory: a directory
    :return: a list of all files
    """

    file_str = os.path.join(directory, 'ensimpl.*.db3')
    dbs = glob.glob(file_str)

    db_vers = []
    db_temp = {}

    # get the versions
    for db in dbs:
        version = int(db.split(".")[-2])
        db_vers.append(version)
        db_temp[version] = db

    db_vers.sort()

    all_sorted_dbs = OrderedDict()

    default = None
    for version in db_vers:
        all_sorted_dbs[version] = db_temp[version]
        default = db_temp[version]

    global ENSIMPL_DBS
    ENSIMPL_DBS = all_sorted_dbs
    global ENSIMPL_DB_DEFAULT
    ENSIMPL_DB_DEFAULT = default


def init(directory=None):
    """
    Initialize the "global" database variables.

    :param str directory: a directory that holds the data files
     if None the environment variable ENSIMPL_DIR will be used
    :return: None
    """
    ensimpl_dir = os.environ.get('ENSIMPL_DIR', None)

    if directory:
        ensimpl_dir = directory

    if not ensimpl_dir:
        print('ENSIMPL_DIR not configured in environment or directory was not supplied as an option')
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
