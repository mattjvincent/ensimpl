# -*- coding: utf_8 -*-
from collections import OrderedDict
import sqlite3

import ensimpl.utils as utils
import ensimpl.fetch.utils as fetch_utils

LOG = utils.get_logger()


def chromosomes(version, species_id=None):
    """Get the chromosomes.

    Args:
        version (int): Ensembl version or None for latest
        species_id (str): the species identifier

    Returns:
        dict: a ``dict`` of lists with each key being a species identifier
            each ``list`` element is another ``dict`` with the following keys:
            chromosome, name, length, order

    """
    sql_chromosomes = 'SELECT * FROM chromosomes '
    sql_where = ' WHERE species_id = ? '
    sql_order = ' ORDER BY species_id, chromosome_num '

    conn = fetch_utils.connect_to_database(version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql_statement = sql_chromosomes

    params = ()

    if species_id:
        sql_statement += sql_where
        params = (species_id, )

    sql_statement += sql_order

    chroms = {}

    for row in cursor.execute(sql_statement, params):
        data = chroms.get(row['species_id'], [])

        data.append({
            'chromosome': row['chromosome'],
            'name': row['chromosome'],
            'length': row['chromosome_length'],
            'order': row['chromosome_num']
        })

        chroms[row['species_id']] = data

    return chroms


def karyotypes(version, species_id=None):
    """Get the karyotypes.

    Args:
        version (int): Ensembl version or None for latest
        species_id (str): the species identifier

    Returns:
        dict: a ``dict`` of lists with each key being a species identifier
            each ``list`` element is another ``dict`` with the following keys:
            karyotype, chromosome, name, length, order

    """
    sql_karyotypes = ('SELECT * FROM karyotypes k, chromosomes c '
                      ' WHERE k.chromosome = c.chromosome'
                      '   AND k.species_id = c.species_id ')
    sql_where = ' AND c.species_id = ? '
    sql_order = ' ORDER BY k.species_id, c.chromosome_num, k.seq_region_start '

    conn = fetch_utils.connect_to_database(version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql_statement = sql_karyotypes

    params = ()

    if species_id:
        sql_statement += sql_where
        params = (species_id, )

    sql_statement += sql_order

    karyotype_data = {}

    for row in cursor.execute(sql_statement, params):
        species_data = karyotype_data.get(row['species_id'], OrderedDict())
        chrom_data = species_data.get(row['chromosome'],
                                      {'chromosome': row['chromosome'],
                                       'name': row['chromosome'],
                                       'length': row['chromosome_length'],
                                       'order': row['chromosome_num'],
                                       'karyotypes': []})
        chrom_data['karyotypes'].append({'seq_region_start': row['seq_region_start'],
                                         'seq_region_end': row['seq_region_end'],
                                         'band': row['band'],
                                         'stain': row['stain']})
        species_data[row['chromosome']] = chrom_data
        karyotype_data[row['species_id']] = species_data

    return karyotype_data


def meta(version):
    """Get the database meta information..

    Args:
        version (int): Ensembl version or None for latest

    Returns:
        dict: a ``dict`` with keys of 'version' and 'species'; each species
            element returns the assembly information
    """
    sql_meta = '''
        SELECT distinct meta_key meta_key, meta_value, species_id
          FROM meta_info
         ORDER BY meta_key
    '''

    conn = fetch_utils.connect_to_database(version)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    meta_temp = {}
    ensembl_version = 'Unknown'

    for row in cursor.execute(sql_meta):
        data = meta_temp.get(row['species_id'], {})
        data[row['meta_key']] = row['meta_value']
        meta_temp[row['species_id']] = data

        if row['meta_key'] == 'version':
            ensembl_version = row['meta_value']

    cursor.close()

    meta_info = {'version': ensembl_version, 'species': {}}

    for (k, v) in meta_temp.items():
        meta_info['species'][k] = {'assembly': v['assembly'],
                                   'assembly_patch': v['assembly_patch']}

    return meta_info


def info(version):
    """Get information for the version.

    Args:
        version (int): Ensembl version or None for latest

    Returns:
        dict: a ``dict`` with keys of 'version' and 'species'; each species
            element returns the assembly information and statistical information
            about the elements in the database
    """
    stats = meta(version)

    sql_lookup_stats = '''
        SELECT count(egl.lookup_value) num, sr.description, egl.species_id 
          FROM ensembl_genes_lookup egl, search_ranking sr
         WHERE egl.ranking_id = sr.ranking_id
         GROUP BY sr.description, egl.species_id 
         ORDER BY sr.score desc
    '''

    conn = fetch_utils.connect_to_database(version)
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
