# -*- coding: utf-8 -*-
"""This module is specific to Ensembl db operations.

Todo:
    * better documentation
"""

from operator import itemgetter

import ensimpl.utils as utils

import pymysql

LOG = utils.get_logger()

SQL_ENSEMBL_65_SELECT_GENE = '''
SELECT g.gene_id,
       s.name seq_id,
       g.seq_region_start,
       g.seq_region_end,
       g.seq_region_strand,
       g.stable_id ensembl_id,
       g.version ensembl_id_version,
       x1.display_label symbol,
       x1.description,
       x2.xref_id,
       x2.dbprimary_acc external_id,
       x2.display_label external_label,
       x2.version external_version,
       ed2.db_name,
       ed2.db_display_name
  FROM gene g,
       xref x1,
       xref x2,
       object_xref o,
       external_db ed1,
       external_db ed2,
       seq_region s
 WHERE g.display_xref_id = x1.xref_id
   AND g.seq_region_id = s.seq_region_id
   AND o.ensembl_id = g.gene_id
   AND o.xref_id = x2.xref_id
   AND x1.external_db_id = ed1.external_db_id
   AND x2.external_db_id = ed2.external_db_id
   AND s.name in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 
                   '11',  '12', '13', '14', '15', '16', '17', '18', '19', '20',
                   '21', '22', 'X', 'Y', 'MT')
'''

SQL_ENSEMBL_65_SELECT_GENE_WHERE = '''
   AND g.stable_id = ?
'''

SQL_ENSEMBL_65_SELECT_GENE_ORDER_BY = '''
 ORDER BY g.stable_id 
'''

SQL_ENSEMBL_48_SELECT_GENE = '''
SELECT g.gene_id,
       s.name seq_id,
       g.seq_region_start,
       g.seq_region_end,
       g.seq_region_strand,
       gs.stable_id ensembl_id,
       gs.version ensembl_id_version,
       x1.display_label symbol,
       x1.description,
       x2.xref_id,
       x2.dbprimary_acc external_id,
       x2.display_label external_label,
       x2.version external_version,
       ed2.db_name,
       ed2.db_display_name
  FROM gene g,
       gene_stable_id gs,
       xref x1,
       xref x2,
       object_xref o,
       external_db ed1,
       external_db ed2,
       seq_region s
 WHERE g.gene_id = gs.gene_id
   AND g.display_xref_id = x1.xref_id
   AND g.seq_region_id = s.seq_region_id
   AND o.ensembl_id = g.gene_id
   AND o.xref_id = x2.xref_id
   AND x1.external_db_id = ed1.external_db_id
   AND x2.external_db_id = ed2.external_db_id
   AND s.name in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 
                   '11',  '12', '13', '14', '15', '16', '17', '18', '19', '20',
                   '21', '22', 'X', 'Y', 'MT')
'''

SQL_ENSEMBL_48_SELECT_GENE_WHERE = '''
   AND gs.stable_id = ?
'''

SQL_ENSEMBL_48_SELECT_GENE_ORDER_BY = '''
 ORDER BY gs.stable_id 
'''

SQL_ENSEMBL_65_SELECT_GTPE = '''
SELECT g.stable_id gene_id,
       g.version gene_version,
       x1.display_label gene_name,
       s.name gene_chrom,
       g.seq_region_start gene_start,
       g.seq_region_end gene_end,
       g.seq_region_strand gene_strand,
       t.stable_id transcript_id,
       t.version transcript_version,
       x2.display_label transcript_name,
       t.seq_region_start transcript_start,
       t.seq_region_end transcript_end,
       (select stable_id 
          from translation 
         where translation.transcript_id = t.transcript_id) protein_id,
       (select version
          from translation 
         where translation.transcript_id = t.transcript_id) protein_version,
       e.stable_id exon_id,
       e.version exon_version,
       e.seq_region_start exon_start,
       e.seq_region_end exon_end,
       et.rank exon_number
  FROM gene g,
       transcript t,
       exon e,
       exon_transcript et,
       seq_region s,
       xref x1,
       xref x2
 WHERE g.gene_id = t.gene_id
   AND g.seq_region_id = s.seq_region_id
   AND t.transcript_id = et.transcript_id
   AND et.exon_id = e.exon_id
   AND g.display_xref_id = x1.xref_id
   AND t.display_xref_id = x2.xref_id
   AND s.name in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 
                   '11',  '12', '13', '14', '15', '16', '17', '18', '19', '20',
                   '21', '22', 'X', 'Y', 'MT')
'''

SQL_ENSEMBL_65_SELECT_GTPE_WHERE = '''
   AND g.stable_id = ?
'''

SQL_ENSEMBL_65_SELECT_GTPE_ORDER_BY = '''
   ORDER BY g.stable_id, t.stable_id, et.rank
'''

SQL_ENSEMBL_48_SELECT_GTPE = '''
SELECT gs.stable_id gene_id,       
       gs.version gene_version,
       x1.display_label gene_name,
       s.name gene_chrom,
       g.seq_region_start gene_start,
       g.seq_region_end gene_end,
       g.seq_region_strand gene_strand,
       ts.stable_id transcript_id,
       ts.version transcript_version,
       x2.display_label transcript_name,
       t.seq_region_start transcript_start,
       t.seq_region_end transcript_end,
       (select tsi.stable_id 
          from translation, translation_stable_id tsi 
         where translation.translation_id = tsi.translation_id 
           and translation.transcript_id = t.transcript_id) protein_id,
       (select tsi.version 
          from translation, translation_stable_id tsi 
         where translation.translation_id = tsi.translation_id 
           and translation.transcript_id = t.transcript_id) protein_version,
       es.stable_id exon_id,
       es.version exon_version,
       e.seq_region_start exon_start,
       e.seq_region_end exon_end,
       et.rank exon_number
  FROM gene g,
       gene_stable_id gs, 
       transcript t,
       transcript_stable_id ts,
       exon e,
       exon_stable_id es,
       exon_transcript et,
       seq_region s,
       xref x1,
       xref x2
 WHERE g.gene_id = t.gene_id
   AND g.gene_id = gs.gene_id
   AND g.seq_region_id = s.seq_region_id
   AND t.transcript_id = et.transcript_id
   AND t.transcript_id = ts.transcript_id
   AND et.exon_id = e.exon_id
   AND e.exon_id = es.exon_id
   AND g.display_xref_id = x1.xref_id
   AND t.display_xref_id = x2.xref_id
   AND s.name in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 
                   '11',  '12', '13', '14', '15', '16', '17', '18', '19', '20',
                   '21', '22', 'X', 'Y', 'MT')
'''

SQL_ENSEMBL_48_SELECT_GTPE_WHERE = '''
   AND gs.stable_id = ?
'''

SQL_ENSEMBL_48_SELECT_GTPE_ORDER_BY = '''
   ORDER BY gs.stable_id, ts.stable_id, et.rank
'''

SQL_ENSEMBL_SELECT_SYNONYMS = '''
SELECT * 
  FROM external_synonym 
'''

SQL_ENSEMBL_SELECT_CHROMOSOME = '''
SELECT DISTINCT sr.name, sr.length, k.seq_region_start, k.seq_region_end, k.band, k.stain
  FROM coord_system cs,
       seq_region sr
  LEFT JOIN karyotype k ON k.seq_region_id = sr.seq_region_id
 WHERE cs.coord_system_id = sr.coord_system_id
   AND sr.name in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 
                   '11',  '12', '13', '14', '15', '16', '17', '18', '19', '20',
                   '21', '22', 'X', 'Y', 'MT')
   AND cs.rank = 1
 ORDER BY cast(replace(replace(replace(sr.name, 'X', '50'), 'Y', '51'), 'MT', '52') as signed), k.seq_region_start
'''


def _get_sql_select_gene(version):
    """Retrieve the proper SQL statement based upon the Ensembl version

    Args:
        version (int): Ensembl version

    Returns:
        str: the SQL statement
    """
    if int(version) < 65:
        return SQL_ENSEMBL_48_SELECT_GENE + SQL_ENSEMBL_48_SELECT_GENE_ORDER_BY
    else:
        return SQL_ENSEMBL_65_SELECT_GENE + SQL_ENSEMBL_65_SELECT_GENE_ORDER_BY


def _get_sql_select_gtpe(version):
    """Retrieve the proper SQL statement based upon the Ensembl version

    Args:
        version (int): Ensembl version

    Returns:
        str: the SQL statement
    """
    if int(version) < 65:
        return SQL_ENSEMBL_48_SELECT_GTPE + SQL_ENSEMBL_48_SELECT_GTPE_ORDER_BY
    else:
        return SQL_ENSEMBL_65_SELECT_GTPE + SQL_ENSEMBL_65_SELECT_GTPE_ORDER_BY


def connect_to_database(ref):
    """Connect to Ensembl.

    Args:
        ref (:obj:`ensimpl.create.create.ensimpl.EnsemblReference`):
            contains information about the Ensembl reference

    Returns:
        a connection to the database
    """
    try:
        LOG.debug('Connecting to {} ...'.format(ref.server))
        connection = pymysql.connect(host=ref.server,
                                     port=int(ref.port),
                                     user=ref.user_id,
                                     password=ref.password,
                                     db=ref.db,
                                     cursorclass=pymysql.cursors.DictCursor)

        LOG.debug('Connected')
        return connection
    except pymysql.Error as e:
        LOG.error('Unable to connect to Ensembl: {}'.format(e))
        raise e


def extract_chromosomes_karyotypes(ref):
    """Extract the chromosomes and karyotypes from Ensembl.

    Args:
        ref (:obj:`ensimpl.create.create.ensimpl.EnsemblReference`):
            contains information about the Ensembl reference

    Returns:
        list: a ``list`` of ``dict``, name and length
    """
    chromosomes = []

    try:
        conn = connect_to_database(ref)

        LOG.debug('Extracting chromosomes and karyotypes...')
        with conn.cursor() as cursor:
            num_rows = cursor.execute(SQL_ENSEMBL_SELECT_CHROMOSOME)
            LOG.debug('{:,} records returned'.format(num_rows))

            for row in cursor:
                chromosomes.append(row)

            LOG.debug('{:,} chromosomes extracted'.format(num_rows))
    except pymysql.Error as e:
        LOG.error('Unable to extract chromosomes and karyotpes from ensembl: {}').format(e)
        return None

    return chromosomes


def extract_synonyms(ref):
    """Extract the synonyms from Ensembl.

    Args:
        ref (:obj:`ensimpl.create.create.ensimpl.EnsemblReference`):
            contains information about the Ensembl reference

    Returns:
        dict: the keys are the xref_id and values are a list of synonyms
    """
    synonyms = {}

    try:
        conn = connect_to_database(ref)

        LOG.debug('Extracting synonyms...')
        with conn.cursor() as cursor:
            count = 0
            num_rows = cursor.execute(SQL_ENSEMBL_SELECT_SYNONYMS)
            LOG.debug('{:,} records returned'.format(num_rows))

            for row in cursor:
                xref = synonyms.get(row['xref_id'], None)

                if not xref:
                    xref = []

                xref.append(row['synonym'])

                synonyms[row['xref_id']] = xref

                if count and count % 10000 == 0:
                    LOG.debug('{:,} synonyms extracted'.format(count))
                count += 1

            LOG.debug('{:,} synonyms extracted'.format(count))
    except pymysql.Error as e:
        LOG.error('Unable to extract synonyms from ensembl: {}').format(e)
        return None

    return synonyms


def extract_ensembl_genes(ref):
    """Extract the gene information from Ensembl.

    Args:
        ref (:obj:`ensimpl.create.create.ensimpl.EnsemblReference`):
            contains information about the Ensembl reference

    Returns:
        dict: gene information with the Ensembl ID being the key

             each gene is another ``dict`` with the following keys

             'ensembl_id', 'ensembl_id_version', 'seq_id',
             'seq_region_start', 'seq_region_end', 'seq_region_strand',
             'symbol', 'description', 'ids'

    """
    gene_items = ['ensembl_id', 'ensembl_id_version', 'seq_id',
                  'seq_region_start', 'seq_region_end', 'seq_region_strand',
                  'symbol', 'description']
    genes = {}

    try:
        conn = connect_to_database(ref)
        sql = _get_sql_select_gene(ref.version)

        LOG.debug('Extracting genes...')
        with conn.cursor() as cursor:
            count = 0
            num_rows = cursor.execute(sql)
            LOG.debug('{:,} records returned'.format(num_rows))

            for row in cursor:
                gene = genes.get(row['ensembl_id'], None)

                if not gene:
                    gene = {'ids': []}
                    for i in gene_items:
                        gene[i] = row[i]

                gene['ids'].append({'xref_id': row['xref_id'],
                                    'external_id': row['external_id'],
                                    'db_name': row['db_name']})

                genes[row['ensembl_id']] = gene

                if count and count % 10000 == 0:
                    LOG.debug('{:,} genes extracted'.format(len(genes)))
                count += 1

            LOG.debug('{:,} genes extracted'.format(len(genes)))
    except pymysql.Error as e:
        LOG.error('Unable to extract genes from ensembl: {}').format(e)
        return None

    return genes


def extract_ensembl_gtpe(ref):
    """Extract the gene, transcript, protein, exon information from Ensembl.

    Args:
        ref (:obj:`ensimpl.create.create.ensimpl.EnsemblReference`):
            contains information about the Ensembl reference

    Returns:
        list: a list of the gene, transcript, protein, exon information
    """
    gtep = []
    genes = {}

    try:
        conn = connect_to_database(ref)
        sql = _get_sql_select_gtpe(ref.version)

        LOG.debug('Extracting transcript, protein, exon information ...')
        with conn.cursor() as cursor:
            count = 0
            num_rows = cursor.execute(sql)
            LOG.debug('{:,} records returned'.format(num_rows))

            for row in cursor:
                gtep.append(row)
                genes[row['gene_id']] = 1

                if count and count % 100000 == 0:
                    LOG.debug('{:,} genes extracted'.format(len(genes)))
                count += 1

            LOG.debug('{:,} genes extracted'.format(len(genes)))
    except pymysql.Error as e:
        LOG.error('Unable to extract genes from ensembl: {}').format(e)
        return None

    return gtep




