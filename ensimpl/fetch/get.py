# -*- coding: utf_8 -*-
from collections import OrderedDict
import sqlite3

import ensimpl.utils as utils
import ensimpl.fetch.utils as fetch_utils

LOG = utils.get_logger()


def chromosomes(version, species_id):
    """Get the chromosomes.

    Args:
        version (int): Ensembl version or None for latest
        species_id (str): the species identifier

    Returns:
        list: a ``list`` element with a ``dict`` with the following keys:
            chromosome, name, length, order

    """
    sql_chromosomes = 'SELECT * FROM chromosomes '
    sql_order = ' ORDER BY chromosome_num '

    conn = fetch_utils.connect_to_database(version, species_id)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql_statement = sql_chromosomes

    params = ()

    sql_statement += sql_order

    chroms = []

    for row in cursor.execute(sql_statement, params):
        chroms.append({
            'chromosome': row['chromosome'],
            'name': row['chromosome'],
            'length': row['chromosome_length'],
            'order': row['chromosome_num']
        })

    return chroms


def karyotypes(version, species_id):
    """Get the karyotypes.

    Args:
        version (int): Ensembl version or None for latest
        species_id (str): the species identifier

    Returns:
        dict: a ``dict`` of chromsomes with a ``dict`` with the following keys:
            karyotype, chromosome, name, length, order

    """
    sql_karyotypes = ('SELECT * FROM karyotypes k, chromosomes c '
                      ' WHERE k.chromosome = c.chromosome ')
    sql_order = ' ORDER BY c.chromosome_num, k.seq_region_start '

    conn = fetch_utils.connect_to_database(version, species_id)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql_statement = sql_karyotypes

    params = ()

    sql_statement += sql_order

    karyotype_data = OrderedDict()

    for row in cursor.execute(sql_statement, params):
        chrom_data = karyotype_data.get(row['chromosome'],
                                      {'chromosome': row['chromosome'],
                                       'name': row['chromosome'],
                                       'length': row['chromosome_length'],
                                       'order': row['chromosome_num'],
                                       'karyotypes': []})
        chrom_data['karyotypes'].append({'seq_region_start': row['seq_region_start'],
                                         'seq_region_end': row['seq_region_end'],
                                         'band': row['band'],
                                         'stain': row['stain']})
        karyotype_data[row['chromosome']] = chrom_data

    return karyotype_data


def meta(version, species_id):
    """Get the database meta information..

    Args:
        version (int): Ensembl version or None for latest
        species_id (str): the species identifier

    Returns:
        dict: a ``dict`` with keys of 'version' and 'species'; each species
            element returns the assembly information
    """
    sql_meta = '''
        SELECT distinct meta_key meta_key, meta_value, species_id
          FROM meta_info
         WHERE species_id = :species_id 
         ORDER BY meta_key
    '''

    conn = fetch_utils.connect_to_database(version, species_id)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    meta_info = {'species': species_id}

    for row in cursor.execute(sql_meta, {'species_id': species_id}):

        for val in ['version', 'assembly', 'assembly_patch']:
            if row['meta_key'] == val:
                meta_info[val] = row['meta_value']

    cursor.close()

    return meta_info


def info(version, species_id):
    """Get information for the version.

    Args:
        version (int): Ensembl version or None for latest
        species_id (str): the species identifier

    Returns:
        dict: a ``dict`` with keys of 'version' and 'species'; each species
            element returns the assembly information and statistical information
            about the elements in the database
    """
    stats = meta(version)

    sql_lookup_stats = '''
        SELECT count(egl.lookup_value) num, sr.description 
          FROM ensembl_genes_lookup egl, search_ranking sr
         WHERE egl.ranking_id = sr.ranking_id
         GROUP BY sr.description, egl.species_id 
         ORDER BY sr.score desc
    '''

    conn = fetch_utils.connect_to_database(version, species_id)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    for row in cursor.execute(sql_lookup_stats):
        data = (row['description'], row['num'])
        if 'stats' not in stats['species'][row['species_id']]:
            stats['species'][row['species_id']]['stats'] = []
        stats['species'][row['species_id']]['stats'].append(data)

    cursor.close()
    conn.close()

    return stats
