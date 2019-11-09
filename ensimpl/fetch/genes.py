# -*- coding: utf_8 -*-
import sqlite3
import time

from collections import OrderedDict

import ensimpl.fetch.get as fetch_get
import ensimpl.fetch.utils as fetch_utils
import ensimpl.utils as utils

LOG = utils.get_logger()

EXTERNAL_DBS = [
    'Ensembl',
    'EntrezGene',
    'Uniprot_gn',
    'MGI',
    'HGNC',
]


SQL_GENES_ALL = '''
SELECT g.ensembl_id match_id,
       g.ensembl_id ensembl_id, 
       g.ensembl_id gene_id,
       g.ensembl_version gene_version,
       g.species_id gene_species_id,
       g.symbol gene_symbol,
       g.name gene_name,
       g.synonyms gene_synonyms,
       g.external_ids gene_external_ids,
       g.chromosome gene_chromosome,
       g.start_position gene_start,
       g.end_position gene_end,
       g.strand gene_strand,
       g.homolog_ids homolog_ids,
       'EG' type_key
  FROM ensembl_genes g
'''

SQL_GENES_FILTERED = '''
SELECT t.ensembl_id match_id,
       g.ensembl_id ensembl_id, 
       g.ensembl_id gene_id,
       g.ensembl_version gene_version,
       g.species_id gene_species_id,
       g.symbol gene_symbol,
       g.name gene_name,
       g.synonyms gene_synonyms,
       g.external_ids gene_external_ids,
       g.chromosome gene_chromosome,
       g.start_position gene_start,
       g.end_position gene_end,
       g.strand gene_strand,
       g.homolog_ids homolog_ids,
       'EG' type_key
  FROM ensembl_genes g,
       (SELECT eg.gene_id, eg.ensembl_id 
          FROM ensembl_gtpe eg
         WHERE eg.ensembl_id in (SELECT ensembl_id 
                                   FROM {})) t       
 WHERE g.ensembl_id = t.gene_id
'''

SQL_GENES_FULL_ALL = '''
SELECT g.ensembl_id match_id,
       g.ensembl_id gene_id,
       g.ensembl_version gene_version,
       g.species_id gene_species_id,
       g.symbol gene_symbol,
       g.name gene_name,
       g.synonyms gene_synonyms,
       g.external_ids gene_external_ids,
       g.chromosome gene_chromosome,
       g.start_position gene_start,
       g.end_position gene_end,
       g.strand gene_strand,
       g.homolog_ids homolog_ids,
       r.*
  FROM ensembl_genes g,
       ensembl_gtpe r
 WHERE g.ensembl_id = r.gene_id
'''

SQL_GENES_FULL_FILTERED = '''
SELECT t.ensembl_id match_id,
       g.ensembl_id gene_id,
       g.ensembl_version gene_version,
       g.species_id gene_species_id,
       g.symbol gene_symbol,
       g.name gene_name,
       g.synonyms gene_synonyms,
       g.external_ids gene_external_ids,
       g.chromosome gene_chromosome,
       g.start_position gene_start,
       g.end_position gene_end,
       g.strand gene_strand,
       g.homolog_ids homolog_ids,
       r.*
  FROM ensembl_genes g,
       ensembl_gtpe r,       
       (SELECT eg.gene_id, eg.ensembl_id 
          FROM ensembl_gtpe eg
         WHERE eg.ensembl_id in (SELECT ensembl_id 
                                   FROM {})) t       
 WHERE g.ensembl_id = r.gene_id
   AND r.gene_id = t.gene_id  
'''


SQL_GENES_ORDER_BY_ID = ' ORDER BY g.ensembl_id'

SQL_GENES_ORDER_BY_POSITION = '''
 ORDER BY cast(
       replace(replace(replace(g.chromosome,'X','50'),'Y','51'),'MT','51') 
       AS int), g.start_position, g.end_position
'''

SQL_HOMOLOGY = '''
SELECT eh.ensembl_id,
       eh.ensembl_version,
       eh.ensembl_symbol,
       eh.perc_id query_id_perc,
       eh.homolog_id,
       eh.homolog_version,
       eh.homolog_symbol,
       eh.homolog_perc_id target_id_perc,
       eh.dn,
       eh.ds,
       eh.goc_score,
       eh.wga_coverage,
       eh.is_high_confidence conf
  FROM ensembl_homologs eh 
 ORDER BY eh.ensembl_id, eh.homolog_id
'''

SQL_HOMOLOGY_FILTERED = '''
SELECT eh.ensembl_id,
       eh.ensembl_version,
       eh.ensembl_symbol,
       eh.perc_id query_id_perc,
       eh.homolog_id,
       eh.homolog_version,
       eh.homolog_symbol,
       eh.homolog_perc_id target_id_perc,
       eh.dn,
       eh.ds,
       eh.goc_score,
       eh.wga_coverage,
       eh.is_high_confidence conf
  FROM ensembl_homologs eh
 WHERE eh.ensembl_id in ({})
 ORDER BY eh.ensembl_id, eh.homolog_id
'''

SQL_IDS_FILTERED = '''
SELECT all_ids.ensembl_id,
       all_ids.external_id,
       all_ids.external_db,
       matches.match_id
  FROM (SELECT ensembl_id, external_id, external_db
          FROM ensembl_gene_ids        
         UNION        
        SELECT ensembl_id, homolog_id external_id, 'Ensembl_homolog' external_db
          FROM ensembl_homologs
       ) all_ids,
       (SELECT distinct ensembl_id, external_id match_id
          FROM ensembl_gene_ids
         WHERE external_id in ({})
           AND external_db = "{}"
       ) matches
 WHERE matches.ensembl_id = all_ids.ensembl_id       
 ORDER BY all_ids.ensembl_id, all_ids.external_db, all_ids.external_id
'''

SQL_IDS_FILTERED_ENSEMBL = '''
SELECT all_ids.ensembl_id,
       all_ids.external_id,
       all_ids.external_db,
       matches.match_id
  FROM (SELECT ensembl_id, external_id, external_db
          FROM ensembl_gene_ids        
         UNION        
        SELECT ensembl_id, homolog_id external_id, 'Ensembl_homolog' external_db
          FROM ensembl_homologs
       ) all_ids,
       (SELECT distinct ensembl_id, ensembl_id match_id
          FROM ensembl_gene_ids
         WHERE ensembl_id in ({})
       ) matches
 WHERE matches.ensembl_id = all_ids.ensembl_id       
 ORDER BY all_ids.ensembl_id, all_ids.external_db, all_ids.external_id
'''

SQL_IDS_ALL = '''
SELECT all_ids.ensembl_id,
       all_ids.external_id,
       all_ids.external_db,
       matches.match_id
  FROM (SELECT ensembl_id, external_id, external_db
          FROM ensembl_gene_ids        
         UNION        
        SELECT ensembl_id, homolog_id external_id, 'Ensembl_homolog' external_db
          FROM ensembl_homologs
       ) all_ids,
       (SELECT distinct ensembl_id, ensembl_id match_id
          FROM ensembl_gene_ids
       ) matches
 WHERE matches.ensembl_id = all_ids.ensembl_id       
 ORDER BY all_ids.ensembl_id, all_ids.external_db, all_ids.external_id
'''


def get_ids(ids=None, species=None, version=None, source_db='Ensembl'):
    results = OrderedDict()

    valid_db_ids = ['Ensembl', 'Ensembl_homolog']
    external_dbs = fetch_get.external_dbs(species, version)

    for db in external_dbs:
        valid_db_ids.append(db['external_db_id'])

    if source_db not in valid_db_ids:
        raise ValueError(f'Valid source dbs are: {",".join(valid_db_ids)}')

    try:
        conn = fetch_utils.connect_to_database(species, version)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        #
        # build the query
        #

        SQL_QUERY = None

        if ids:
            in_values = '","'.join(ids)
            in_values = f'"{in_values}"'

            if source_db and source_db.lower() == 'ensembl':
                SQL_QUERY = SQL_IDS_FILTERED_ENSEMBL
                SQL_QUERY = SQL_QUERY.format(in_values)
            else:
                SQL_QUERY = SQL_IDS_FILTERED
                SQL_QUERY = SQL_QUERY.format(in_values, source_db)

        else:
            SQL_QUERY = SQL_IDS_ALL


        #
        # execute the query
        #

        for row in cursor.execute(SQL_QUERY, {}):

            match_id = row['match_id']

            match = results.get(match_id,
                                {'Ensembl': [row['ensembl_id']]})

            id_arr = match.get(row['external_db'], [])
            id_arr.append(row['external_id'])

            match[row['external_db']] = id_arr

            results[match_id] = match

    except sqlite3.Error as e:
        raise Exception(e)

    return results


def get_homology(ids=None, species=None, version=None):
    results = OrderedDict()

    try:
        conn = fetch_utils.connect_to_database(species, version)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        #
        # build the query
        #

        SQL_QUERY = None

        if ids:
            SQL_QUERY = SQL_HOMOLOGY_FILTERED

            in_values = '","'.join(ids)
            in_values = f'"{in_values}"'

            # create a temp table and insert into
            SQL_QUERY = SQL_QUERY.format(in_values)
        else:
            SQL_QUERY = SQL_HOMOLOGY


        #
        # execute the query
        #

        for row in cursor.execute(SQL_QUERY, {}):
            gene_id = row['ensembl_id']

            gene = results.get(gene_id)

            if not gene:
                gene = []

            gene.append(utils.dictify_row(cursor,row))

            results[gene_id] = gene

    except sqlite3.Error as e:
        raise Exception(e)

    return results


def get(ids=None, species=None, version=None, order='id', details=False):
    """Get genes matching the ids.

        Each match object will contain:

        =================  =======  ============================================
        Element            Type     Description
        =================  =======  ============================================
        ensembl_id         string   Ensembl gene identifier
        ensembl_version    integer  version of the identifier
        species_id         string   species identifier: 'Mm', 'Hs', etc
        chromosome         string   the chromosome
        start              integer  start position in base pairs
        end                integer  end position in base pairs
        strand             string   '+' or '-'
        gene_name          string   name of the gene
        gene_symbol        string   gene symbol
        gene_synonyms      list     list of strings
        gene_external_ids  list     each having keys of 'db' and 'db_id'
        homolog_ids        list     each having keys of 'homolog_id' and
                                    'homolog_symbol'
        =================  =======  ============================================

        If ``full`` is ``True``, each match will also contain the following:

        ``transcripts``, with each item containing:

        =================  =======  ============================================
        Element            Type     Description
        =================  =======  ============================================
        id                 string   Ensembl gene identifier
        ensembl_version    integer  version of the identifier
        symbol             string   transcript symbol
        start              integer  start position in base pairs
        end                integer  end position in base pairs
        exons              list     dict of: number,id,start,end,ensembl_version
        protein            dict     id, start, end, ensembl_version
        =================  =======  ============================================

    Args:
        ids (list): A ``list`` of ``str`` which are Ensembl identifiers.
        version (int): The Ensembl version number.
        species (str): The Ensembl species identifier.
        order (str): Order by 'id' or 'position'.
        details (bool): True to retrieve all information including transcripts,
            exons, proteins.  False will only retrieve the top level gene
            information.

    Returns:
        list: A ``list`` of ``dicts`` representing genes.

    Raises:
        Exception: When sqlite error or other error occurs.
    """
    results = OrderedDict()

    try:
        conn = fetch_utils.connect_to_database(species, version)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        #
        # build the query
        #

        SQL_QUERY = None

        if ids:
            if details:
                SQL_QUERY = SQL_GENES_FULL_FILTERED
            else:
                SQL_QUERY = SQL_GENES_FILTERED

            # create a temp table and insert into
            temp_table = f'lookup_ids_{utils.create_random_string()}'

            SQL_TEMP = (f'CREATE TEMPORARY TABLE {temp_table} ( '
                        'ensembl_id TEXT, '
                        'PRIMARY KEY (ensembl_id) );')

            cursor.execute(SQL_TEMP)

            SQL_TEMP = f'INSERT INTO {temp_table} VALUES (?);'
            _ids = [(_,) for _ in ids]
            cursor.executemany(SQL_TEMP, _ids)

            # make sure we add the temp table name to the query
            SQL_QUERY = SQL_QUERY.format(temp_table)
        else:
            if details:
                SQL_QUERY = SQL_GENES_FULL_ALL
            else:
                SQL_QUERY = SQL_GENES_ALL

        if order and order.lower() == 'position':
            SQL_QUERY = f'{SQL_QUERY} {SQL_GENES_ORDER_BY_POSITION}'
        else:
            SQL_QUERY = f'{SQL_QUERY} {SQL_GENES_ORDER_BY_ID}'


        #
        # execute the query
        #

        for row in cursor.execute(SQL_QUERY, {}):
            gene_id = row['gene_id']
            ensembl_id = row['ensembl_id']
            match_id = row['match_id']

            gene = results.get(match_id)

            if not gene:
                gene = {'id': gene_id, 'transcripts': {}}

            if row['type_key'] == 'EG':
                gene['species_id'] = row['gene_species_id']
                gene['chromosome'] = row['gene_chromosome']
                gene['start'] = row['gene_start']
                gene['end'] = row['gene_end']
                gene['strand'] = '+' if row['gene_strand'] > 0 else '-'

                if row['gene_version']:
                    gene['ensembl_version'] = row['gene_version']

                if row['gene_symbol']:
                    gene['symbol'] = row['gene_symbol']

                if row['gene_name']:
                    gene['name'] = row['gene_name']

                if row['gene_synonyms']:
                    row_synonyms = row['gene_synonyms']
                    gene['synonyms'] = row_synonyms.split('||')

                if row['gene_external_ids']:
                    row_external_ids = row['gene_external_ids']
                    external_ids = []
                    if row_external_ids:
                        tmp_external_ids = row_external_ids.split('||')
                        for e in tmp_external_ids:
                            elem = e.split('/')
                            external_ids.append({'db': elem[0], 'db_id': elem[1]})
                    gene['external_ids'] = external_ids

                if row['homolog_ids']:
                    row_homolog_ids = row['homolog_ids']
                    homolog_ids = []
                    if row_homolog_ids:
                        tmp_homolog_ids = row_homolog_ids.split('||')
                        for e in tmp_homolog_ids:
                            elem = e.split('/')
                            homolog_ids.append({'id': elem[0],
                                                'symbol': elem[1]})
                    gene['homolog_ids'] = homolog_ids

            elif row['type_key'] == 'ET':
                transcript_id = row['transcript_id']
                transcript = {'id': transcript_id, 'exons': {}}

                if row['ensembl_id_version']:
                    transcript['version'] = row['ensembl_id_version']

                if row['ensembl_symbol']:
                    transcript['symbol'] = row['ensembl_symbol']

                transcript['start'] = row['start']
                transcript['end'] = row['end']

                gene['transcripts'][transcript_id] = transcript

            elif row['type_key'] == 'EE':
                transcript_id = row['transcript_id']
                transcript = gene['transcripts'].get(transcript_id,
                                                     {'id': transcript_id,
                                                      'exons': {}})

                exon = {'id': ensembl_id,
                        'start': row['start'],
                        'end': row['end'],
                        'number': row['exon_number']}

                if row['ensembl_id_version']:
                    exon['version'] = row['ensembl_id_version']

                transcript['exons'][ensembl_id] = exon

                gene['transcripts'][transcript_id] = transcript

            elif row['type_key'] == 'EP':
                transcript_id = row['transcript_id']
                transcript = gene['transcripts'].get(transcript_id,
                                                     {'id': transcript_id,
                                                      'exons': {}})

                transcript['protein'] = {'id': ensembl_id,
                                         'start': row['start'],
                                         'end': row['end']}

                if row['ensembl_id_version']:
                    transcript['protein']['version'] = row['ensembl_id_version']

                gene['transcripts'][transcript_id] = transcript
            else:
                LOG.error('Unknown')

            results[match_id] = gene

        cursor.close()

        if details:
            homologs = get_homology(ids, species, version)

            # convert transcripts, etc to sorted list rather than dict
            ret = OrderedDict()
            for (gene_id, gene) in results.items():
                if gene:
                    t = []
                    for (transcript_id, transcript) in gene['transcripts'].items():
                        e = []
                        for (exon_id, exon) in transcript['exons'].items():
                            e.append(exon)
                        transcript['exons'] = sorted(e, key=lambda ex: ex['number'])
                        t.append(transcript)
                    gene['transcripts'] = sorted(t, key=lambda tr: tr['start'])

                gene['homologs'] = homologs.get(gene_id, None)

                ret[gene_id] = gene
            results = ret
        else:
            ret = OrderedDict()
            for (gene_id, gene) in results.items():
                if gene:
                    del gene['transcripts']
                ret[gene_id] = gene

            results = ret

        cursor.close()
        conn.close()

    except sqlite3.Error as e:
        raise Exception(e)

    return results






