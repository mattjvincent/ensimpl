# -*- coding: utf_8 -*-
import re
import sqlite3

import ensimpl.utils as utils
import ensimpl.db_config as db_config

LOG = utils.get_logger()

REGEX_ENSEMBL_MOUSE_ID = re.compile('ENSMUS([EGTP])[0-9]{11}', re.IGNORECASE)
REGEX_ENSEMBL_HUMAN_ID = re.compile('ENS([EGTP])[0-9]{11}', re.IGNORECASE)
REGEX_MGI_ID = re.compile('MGI:[0-9]{1,}', re.IGNORECASE)
REGEX_REGION = re.compile('(CHR|)*\s*([0-9]{1,2}|X|Y|MT)\s*(-|:)?\s*(\d+)\s*(MB|M|K|)?\s*(-|:|)?\s*(\d+|)\s*(MB|M|K|)?', re.IGNORECASE)


class Region:
    """Encapsulates a genomic region.

    Attributes:
        chromosome (str): The chromosome name.
        start_position (int): The start position.
        end_position (int): The end position.
    """
    def __init__(self):
        """Initialization."""
        self.chromosome = ''
        self.start_position = None
        self.end_position = None

    def __str__(self):
        """Return string representing this region.

        Returns:
            str: In the format of chromosome:start_position-end_position.
        """
        return f'{self.chromosome}:{self.start_position}-{self.end_position}'

    def __repr__(self):
        """Internal representation.

        Returns:
            str: The keys being the attributes.
        """
        return (f'{self.__class__}({self.chromosome}:'
                f'{self.start_position}-{self.end_position})')


def connect_to_database(release=None, species=None):
    """Connect to the Ensimpl database.

    Args:
        release (str): The Ensembl release, None defaults to latest.
        species (str): The Ensembl species identifier, None defaults to 'Mm'.

    Returns:
        a connection to the database
    """
    try:
        species = 'Mm' if species is None else species

        if release is None:
            release = max(db['release'] for db in db_config.ENSIMPL_DBS)

        database = db_config.get_ensimpl_db(release, species)['db']

        return sqlite3.connect(database)
    except Exception as e:
        LOG.error(f'Error connecting to database: {e}')
        raise e


def nvl(value, default):
    """Returns `value` if value has a value, else `default`.

    Args:
        value: The value to evaluate.
        default: The default value.

    Returns:
        Either `value` or `default`.
    """
    return value if value else default


def nvli(value, default):
    """Returns `value` as an int if `value` can be converted, else `default`.

    Args:
        value: The value to evaluate and convert to an it.
        default: The default value.

    Returns:
        Either `value` or `default`.
    """
    ret = default
    if value:
        try:
            ret = int(value)
        except ValueError:
            pass
    return ret


def get_multiplier(factor):
    """Get multiplying factor.

    The factor value should be 'mb', 'm', or 'k' and the correct multiplier
    will be returned.

    Args:
        factor (str): One of 'mb', 'm', or 'k'.

    Returns:
        int: The multiplying value.
    """
    if factor:
        factor = factor.lower()

        if factor == 'mb':
            return 10000000
        elif factor == 'm':
            return 1000000
        elif factor == 'k':
            return 1000

    return 1


def str_to_region(location):
    """Parse a string into a genomic location.

    Args:
        location (str): The genomic location (range).

    Returns:
        Region: A region object.

    Raises:
        ValueError: If `location` is invalid.
    """
    if not location:
        raise ValueError('No location specified')

    valid_location = location.strip()

    if ('-' not in valid_location) and (' ' not in valid_location) and \
            (':' not in valid_location):
        raise ValueError('Incorrect location format')

    if len(valid_location) <= 0:
        raise ValueError('Empty location')

    match = REGEX_REGION.match(valid_location)

    if not match:
        raise ValueError('Invalid location string')

    loc = Region()
    loc.chromosome = match.group(2)
    loc.start_position = match.group(4)
    loc.end_position = match.group(7)
    multiplier_one = match.group(5)
    multiplier_two = match.group(8)

    loc.start_position = int(loc.start_position)
    loc.end_position = int(loc.end_position)

    if multiplier_one:
        loc.start_position *= get_multiplier(multiplier_one)

    if multiplier_two:
        loc.end_position *= get_multiplier(multiplier_two)

    return loc


def is_valid_region(term):
    """Check if a string can be parsed into a genomic location.

    Args:
        term (str): The genomic location (range).

    Returns:
        bool: True if valid region, False otherwise
    """
    try:
        if ('-' not in term) and (' ' not in term) and (':' not in term):
            raise ValueError('not correct format')

        region = str_to_region(term)

        if region.chromosome is None or \
                region.start_position is None or \
                region.end_position is None:
            return False

    except ValueError as ve:
        return False

    return True


def validate_ensembl_id(ensembl_id):
    """Validate an id to make sure it conforms to the convention.

    Args:
        ensembl_id (str): The Ensembl identifer to test.

    Returns:
        str: The Ensembl id.

    Raises:
        ValueError: If `ensembl_id` is invalid.
    """
    if not ensembl_id:
        raise ValueError('No Ensembl ID')

    valid_id = ensembl_id.strip()

    if len(valid_id) <= 0:
        raise ValueError('Empty Ensembl ID')

    if REGEX_ENSEMBL_HUMAN_ID.match(valid_id):
        return valid_id
    elif REGEX_ENSEMBL_MOUSE_ID.match(valid_id):
        return valid_id

    raise ValueError(f'Invalid Ensembl ID: {ensembl_id}')


