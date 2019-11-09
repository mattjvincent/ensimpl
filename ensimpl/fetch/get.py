# -*- coding: utf_8 -*-
from collections import OrderedDict
import sqlite3

import ensimpl.utils as utils
import ensimpl.fetch.utils as fetch_utils

LOG = utils.get_logger()


def chromosomes(species_id=None, version=None):
    """Get the chromosomes.

    Args:
        species_id (str): The Ensembl species identifier.
        version (int): The Ensembl version.

    Returns:
        list: A ``list`` of ``dicts`` with the following keys:
            * chromosome
            * length
            * order

    """
    sql_statement = 'SELECT * FROM chromosomes ORDER BY chromosome_num '

    conn = fetch_utils.connect_to_database(species_id, version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    chroms = []

    for row in cursor.execute(sql_statement):
        chroms.append({
            'chromosome': row['chromosome'],
            'length': row['chromosome_length'],
            'order': row['chromosome_num']
        })

    return chroms


def karyotypes(species_id=None, version=None):
    """Get the karyotypes.

    Args:
        species_id (str): the species identifier
        version (int): Ensembl version or None for latest

    Returns:
        list: A ``list`` element with a ``dict`` with the following keys:
            * chromosome
            * length
            * order
            * karyotypes
                * seq_region_start
                * seq_region_end
                * band
                * stain
    """
    sql_statement = '''
        SELECT * 
          FROM karyotypes k, chromosomes c
         WHERE k.chromosome = c.chromosome
        ORDER BY c.chromosome_num, k.seq_region_start
    '''

    conn = fetch_utils.connect_to_database(species_id, version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    karyotype_data = OrderedDict()

    for row in cursor.execute(sql_statement):
        chrom_data = karyotype_data.get(row['chromosome'],
                                        {'chromosome': row['chromosome'],
                                         'length': row['chromosome_length'],
                                         'order': row['chromosome_num'],
                                         'karyotypes': []})

        chrom_data['karyotypes'].append({'seq_region_start': row['seq_region_start'],
                                         'seq_region_end': row['seq_region_end'],
                                         'band': row['band'],
                                         'stain': row['stain']})

        karyotype_data[row['chromosome']] = chrom_data

    # turn into a list
    return list(karyotype_data.values())


def meta(species_id=None, version=None):
    """Get the database meta information..

    Args:
        species_id (str): Ensembl species identifier.
        version (int): Ensembl version or None for latest.

    Returns:
        dict: A ``dict`` with the following keys:
            * assembly
            * assembly_patch
            * species
            * version
    """
    sql_meta = '''
        SELECT distinct meta_key meta_key, meta_value, species_id
          FROM meta_info
         ORDER BY meta_key
    '''

    conn = fetch_utils.connect_to_database(species_id, version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    meta_data = {}

    for row in cursor.execute(sql_meta):
        meta_data['species'] = row['species_id']

        for val in ['version', 'assembly', 'assembly_patch', 'url']:
            if row['meta_key'] == val:
                meta_data[val] = row['meta_value']

    cursor.close()

    return meta_data


def info(species_id=None, version=None):
    """Get information for the version.

    Args:
        species_id (str): the species identifier
        version (int): Ensembl version or None for latest

    Returns:
        dict: A ``dict`` with the following keys:
            * assembly
            * assembly_patch
            * species
            * stats - informational counts about the database
            * version
    """
    stats = meta(species_id, version)

    sql_lookup_stats = '''
        SELECT count(egl.lookup_value) num, sr.description 
          FROM ensembl_genes_lookup egl, search_ranking sr
         WHERE egl.ranking_id = sr.ranking_id
         GROUP BY sr.description, egl.species_id 
         ORDER BY sr.score desc
    '''

    conn = fetch_utils.connect_to_database(species_id, version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    stats['stats'] = {}

    for row in cursor.execute(sql_lookup_stats):
        stats['stats'][row['description']] = row['num']

    cursor.close()
    conn.close()

    return stats


def external_dbs(species_id=None, version=None):
    """Get the chromosomes.

    Args:
        species_id (str): The Ensembl species identifier.
        version (int): The Ensembl version.

    Returns:
        list: A ``list`` of ``dicts`` with the following keys:
            * external_db_id
            * external_db_name
            * ranking_id

    """
    sql_statement = 'SELECT * FROM external_dbs ORDER BY external_db_key '

    conn = fetch_utils.connect_to_database(species_id, version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    ext_dbs = []

    species_id = fetch_utils.nvl(species_id, 'Mm')

    for row in cursor.execute(sql_statement):

        if species_id.lower() == 'hs' and row['ranking_id'] == 'MI':
            continue
        elif species_id.lower() == 'mm' and row['ranking_id'] == 'HG':
            continue

        ext_dbs.append({
            'external_db_id': row['external_db_id'],
            'external_db_name': row['external_db_name'],
            'ranking_id': row['ranking_id']
        })

    return ext_dbs

