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

from operator import itemgetter as ig
from functools import cmp_to_key

from urllib.request import urlopen

logging.basicConfig(format='[ENsimpl] [%(asctime)s] %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class ReverseProxied(object):
    """Wrap the application in this middleware and configure the front-end
    server to add these headers, to let you quietly bind this to a URL other
    than / and to an HTTP scheme that is different than what is used locally.

    Taken from: http://flask.pocoo.org/snippets/35/

    Example:
        In Apache::

            <Proxy http://server:port>
                Header add X_FORWADED_PATH "/myprefix"
                RequestHeader set X_FORWARDED_PATH "/myprefix"
            </Proxy>

            ProxyPass /myprefix http://server:port disablereuse=On
            ProxyPassReverse /myprefix http://server:port

        In nginx::

            location /myprefix {
                proxy_pass http://192.168.0.1:5001;
                proxy_set_header Host $host;
                proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                proxy_set_header X-Scheme $scheme;
                proxy_set_header X-Script-Name /myprefix;
            }
    """
    def __init__(self, app):
        """Constructor.

        Args:
            app: The wsgi environment.
        """
        self.app = app

    def __call__(self, environ, start_response):
        """
        Middleware call.

        Args:
            environ (dict): The WSGI environment.
                See :func:`flask.Flask.wsgi_app`
            start_response: The response method.

        Returns:
            :class:`flask.Flask` application.
        """
        script_name = environ.get('HTTP_X_FORWARDED_PATH', '')

        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')

        if scheme:
            environ['wsgi.url_scheme'] = scheme

        return self.app(environ, start_response)


def get_logger():
    """Get the logger.

    Returns:
        logging.Logger: The logging object.
    """
    return logging.getLogger(__name__)


def configure_logging(level=0):
    """Configure the logger with the specified `level`. Valid `level` values
    are:

    ======  =================================
    level   logging value
    ======  =================================
    0       logging.WARNING is informational
    1       logging.INFO is user debug
    2+      logging.DEBUG is developer debug
    ======  =================================

    Anything greater than ``2`` is treated as ``2``.

    Args:
        level (int, optional): The logging level; defaults to ``0``.
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
        cursor (sqlite3.Cursor): The database cursor.
        row (sqlite3.Row): The current row.

    Returns:
        collections.OrderedDict: A ``dict`` with keys as column names.
    """
    d = OrderedDict()
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def dictify_cursor(cursor):
    """All rows are converted into a :class:`collections.OrderedDict` where
    keys are the column names.

    Args:
        cursor (sqlite3.Cursor): The database cursor.

    Returns:
        list: A ``list`` of ``dicts`` where each ``dict's`` key is a column
        name.
    """
    return [dictify_row(cursor, row) for row in cursor]


def cmp(value_1, value_2):
    """Compare two values.  The return value will be ``-1`` if
    `value_1` is less than `value_2`, ``0`` if `value_1` is equal
    to `value_2`, or ``1`` if `value_1` is greater than `value_2`.

    Args:
        value_1: The first value.
        value_2: The second value.

    Returns:
        int:  ``-1``, ``0``, or ``1``
    """
    return (value_1 > value_2) - (value_1 < value_2)


def create_random_string(size=6, chars=string.ascii_uppercase + string.digits):
    """Generate a random string of length `size` using the characters
    specified by `chars`.

    Args:
        size (int, optional): The length of the string.  6 is the default.
        chars (list, optional): The characters to use.

    Returns:
        str: A random generated string.
    """
    return ''.join(random.choice(chars) for _ in range(size))


def delete_file(file_name):
    """ Delete specified `file_name`.  This will fail silently.

    Args:
        file_name (str): The name of file to delete.
    """
    try:
        os.remove(file_name)
    except OSError as ose:
        pass


def format_time(start, end):
    """Format length of time between start and end into a readable string.

    Args:
        start (float): The start time.
        end (float): The end time.

    Returns:
        str: A formatted string of hours, minutes, and seconds.
    """
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)


def get_file_name(url, directory=None):
    """Get the file name of `url`.  If `directory` has a value, return the
    name of the file with directory prepended.

    Examples:
        >>> get_file_name('http://www.google.com/a/b/c/file.html', '/tmp')
        '/tmp/file.html

    Args:
        url (str): The url of a file.
        directory (str, optional): The name of a directory.

    Returns:
        str: The full path of file.
    """
    download_file_name = url.split('/')[-1]
    local_directory = directory if directory else os.getcwd()
    return os.path.abspath(os.path.join(local_directory, download_file_name))


def is_url(url):
    """Check if this is a URL or not by just checking the protocol.

    Args:
        url (str): A string in the form of a url.

    Returns:
        bool: ``True`` if `url` has a valid protocol, ``False`` otherwise.
    """
    if url and url.startswith(('http://', 'https://', 'ftp://')):
        return True

    return False


def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy.

    Args:
        x (dict): Dictionary 1.
        y (dict): Dictionary 2.

    Returns:
        dict: The merged dictionary.
    """
    z = x.copy()
    z.update(y)
    return z


def multikeysort(items, columns):
    """Sort a ``list`` of ``dicts`` by multiple keys in ascending or descending
    order. To sort in descending order, prepend a '-' (minus sign) on the
    column name.

    Pulled from: https://stackoverflow.com/questions/1143671/python-sorting-list-of-dictionaries-by-multiple-keys

    Examples:
        >>> my_list = [
            {'name': 'apple', 'count': 10, 'price': 1.00},
            {'name': 'banana', 'count': 5, 'price': 1.00},
            {'name': 'orange', 'count': 20, 'price': 2.00},
        ]

        >>> multikeysort(my_list, ['-name', 'count'])
        [{'count': 20, 'name': 'orange', 'price': 2.0},
         {'count': 5, 'name': 'banana', 'price': 1.0},
         {'count': 10, 'name': 'apple', 'price': 1.0}]

        >>> multikeysort(my_list, ['-price', 'count'])
        [{'count': 20, 'name': 'orange', 'price': 2.0},
         {'count': 5, 'name': 'banana', 'price': 1.0},
         {'count': 10, 'name': 'apple', 'price': 1.0}]

    Args:
        items (list): The ``list`` of ``dict`` objects.
        columns (list): A ``list`` of columns names to sort `items`.

    Returns:
        list: The sorted ``list``.

    """
    comparers = [
        ((ig(col[1:].strip()), -1) if col.startswith('-') else (
        ig(col.strip()), 1))
        for col in columns
    ]

    def comparer(left, right):
        comparer_iter = (
            cmp(fn(left), fn(right)) * mult
            for fn, mult in comparers
        )
        return next((result for result in comparer_iter if result), 0)

    return sorted(items, key=cmp_to_key(comparer))


def open_resource(resource, mode='rb'):
    """Open different types of files and return a handle to that resource.
    Valid types of resources are gzipped and bzipped files along with URLs.

    Args:
        resource (str): A string representing a file or url.
        mode (str, optional): Mode to open the file.

    Returns:
        A handle to the opened ``resource``.
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


def str2bool(val):
    """Convert a string into a boolean.  Valid strings that return ``True``
    are: ``true``, ``1``, ``t``, ``y``, ``yes``

    Canse-sensitivity does NOT matter.  ``yes`` is the same as ``YeS``.

    Args:
        val (str): A string representing the boolean value of ``True``.

    Returns:
        bool: ``True`` if `val` represents a boolean ``True``.
    """
    return str(val).lower() in ['true', '1', 't', 'y', 'yes']

