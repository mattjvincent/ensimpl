# -*- coding: utf-8 -*-
"""Useful generic utilities for the package.
"""
from collections import OrderedDict
import bz2
import gzip
import logging
import os
import random
import string

from urllib.request import urlopen

logging.basicConfig(format='[ENsimpl] [%(asctime)s] %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


def get_logger():
    """Get the logger.

    Returns:
        logger: the logging object
    """
    return logging.getLogger(__name__)


def configure_logging(level=0):
    """Configure the logger with the specified level.

        0 = logging.WARNING is informational
        1 = logging.INFO is user debug
        2> = logging.DEBUG is developer debug

    Args:
        level (int): the logging level; defaults to 0
    """
    if level == 0:
        get_logger().setLevel(logging.WARN)
    elif level == 1:
        get_logger().setLevel(logging.INFO)
    elif level > 1:
        get_logger().setLevel(logging.DEBUG)


def dictify_row(cursor, row):
    """Turns the given row into a dictionary where the keys are the column names.

    Args:
        cursor (sqlite3.Cursor): the database cursor
        row (dict): the current row

    Returns:
        OrderedDict: dictionary with keys being column names
    """
    d = OrderedDict()
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def dictify_cursor(cursor):
    """
    Converts all cursor rows into dictionaries where keys are the column names.

    Args:
        cursor (sqlite3.Cursor): the database cursor

    Returns:
        list: list of ``OrderedDict`` objects with keys being column names
    """
    return [dictify_row(cursor, row) for row in cursor]


def format_time(start, end):
    """Format length of time between start and end.

    Args:
        start (float): the start time
        end (float): the end time

    Returns:
        str: a formatted string of hours, minutes, and seconds
    """
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)


def str2bool(val):
    """Convert a string into a boolean.

    Args:
        val (str): a string representing True

    Returns:
        bool: True if ``val`` is 'true', '1', 't', 'y', or 'yes'
    """
    return str(val).lower() in ['true', '1', 't', 'y', 'yes']


def open_resource(resource, mode='rb'):
    """Open different types of files and return the handle.

    Args:
        resource (str): a string representing a file or url
        mode (str): standard file open modes

    Returns:
        a handle to the opened ``resource``
    """
    if not resource:
        return None

    if not isinstance(resource, str):
        return resource

    if resource.endswith(('.gz', '.Z', '.z')):
        return gzip.open(resource, mode)
    elif resource.endswith(('.bz', '.bz2', '.bzip2')):
        return bz2.BZ2File(resource, mode)
    elif resource.startswith(('http://', 'https://', 'ftp://')):
        return urlopen(resource)
    else:
        return open(resource, mode)


def get_file_name(url, directory=None):
    """Get the last part of the file name.

    Args:
        url (str): url of file
        directory: (optional) directory name

    Returns:
        str: full path of file
    """
    download_file_name = url.split('/')[-1]
    local_directory = directory if directory else os.getcwd()
    return os.path.abspath(os.path.join(local_directory, download_file_name))


def is_url(url):
    """Check if this is a URL or not by just checking the protocol

    Args:
        url (str): a string in the form of a url

    Returns:
        bool: True if it has a valid protocol
    """
    if url and url.startswith(('http://', 'https://', 'ftp://')):
        return True

    return False


def delete_file(file_name):
    """ Delete specified ``file_name``.

    Args:
        file_name (str): name of file to delete
    """
    try:
        os.remove(file_name)
    except:
        pass


def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z


def create_random_string(size=6, chars=string.ascii_uppercase + string.digits):
    """Generate a random string.

    Args:
        size (int): defaults to 6 characters
        chars (list): ``list`` of characters to use

    Returns:
        a random genereated string
    """
    return ''.join(random.choice(chars) for _ in range(size))
