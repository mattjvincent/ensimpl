# -*- coding: utf-8 -*-
"""This module is specific to ensimpl db operations.
"""
import sqlite3
import time

import ensimpl.utils as utils

LOG = utils.get_logger()

RANKING_ID = {'EntrezGene': 'ZG',
              'MGI': 'MI',
              'UniGene': 'UG',
              'HGNC': 'HG'}


def initialize(db):
    """Initialize the ensimpl database.

    Args:
        db (str): Full path to the database file.
    """
    LOG.info('Initializing database: {}'.format(db))

    start = time.time()
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    LOG.info('Generating tables...')
    for sql in SQL_CREATE_TABLES:
        LOG.debug(sql)
        cursor.execute(sql)

    LOG.info('Initializing tables...')
    for sql in SQL_TABLES_INITIALIZE:
        LOG.debug(sql)
        cursor.execute(sql)

    cursor.close()
    conn.commit()
    conn.close()

    LOG.info('Database initialized in: {}'.format(
        utils.format_time(start, time.time())))


def insert_chromosomes_karyotypes(db, ref, chromosomes):
    """Insert chromosome and karyotype information into the database.

    Args:
        db (str): Name of the database file.

        ref (:obj:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

        chromosomes (list): Each element is a ``dict``.  See
            :func:`ensimpl.create.ensembl_db.extract_chromosomes_karyotypes`
            for more information.

    """
    LOG.info('Inserting chromosomes into database: {}'.format(db))

    start = time.time()
    conn = sqlite3.connect(db)

    sql_chromosomes_insert = ('INSERT INTO chromosomes_tmp '
                              'VALUES (?, ?, ?, ?)')

    sql_karyotypes_insert = ('INSERT INTO karyotypes_tmp '
                             'VALUES (?, ?, ?, ?, ?, ?, ?)')

    chromosome_temp = {}
    karyotype_data = []

    for counter, chromosome in enumerate(chromosomes):
        if chromosome['name'] not in chromosome_temp:
            chromosome_temp[chromosome['name']] = (len(chromosome_temp) + 1,
                                                   chromosome['name'],
                                                   chromosome['length'],
                                                   ref.species_id)
        karyotype_data.append((counter + 1,
                               chromosome['name'],
                               chromosome['seq_region_start'],
                               chromosome['seq_region_end'],
                               chromosome['band'],
                               chromosome['stain'],
                               ref.species_id))

    chromosome_data = []
    for k, v in chromosome_temp.items():
        chromosome_data.append((v))

    cursor = conn.cursor()
    LOG.debug('Inserting {:,} karyotypes...'.format(len(karyotype_data)))
    cursor.executemany(sql_karyotypes_insert, karyotype_data)

    LOG.debug('Inserting {:,} chromosomes...'.format(len(chromosome_data)))
    cursor.executemany(sql_chromosomes_insert, chromosome_data)

    cursor.close()
    conn.commit()
    conn.close()

    LOG.info('Chromosomes and karyotpes inserted in: {}'.format(
        utils.format_time(start, time.time())))


def insert_genes(db, ref, genes, synonyms):
    """Insert genes into the database.

    Args:
        db (str): Name of the database file.

        ref (:obj:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

        genes (dict): Gene information withe Ensembl ID being the key.  Values
            were extracted via the following method:
            :func:`ensimpl.create.ensembl_db.extract_ensembl_genes`.

        synonyms (dict): Synonym information with xref_id being the key. Values
            were extracted via the following method:
            :func:`ensimpl.create.ensembl_db.extract_synonyms`.
    """
    LOG.info('Inserting genes into database: {}'.format(db))

    start = time.time()
    conn = sqlite3.connect(db)

    sql_genes_insert = 'INSERT INTO ensembl_genes_tmp ' + \
                       'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    sql_genes_lookup_insert = 'INSERT INTO ensembl_genes_lookup_tmp ' + \
                              'VALUES (?, ?, ?, ?);'

    gene_data = []
    gene_lookup_data = []
    counter = 0

    species_id = ref.species_id

    for gene_id, gene in sorted(genes.items()):

        ensembl_id = gene.get('ensembl_id', None)
        ensembl_id_version = gene.get('ensembl_id_version', None)
        symbol = gene.get('symbol', None)
        description = gene.get('description', None)
        seq_id = gene.get('seq_id', None)
        seq_start = gene.get('seq_region_start', None)
        seq_end = gene.get('seq_region_end', None)
        strand = gene.get('seq_region_strand', 0)

        ids_text = None
        synonyms_text = None

        ids = gene.get('ids', None)
        if ids:
            ids_tmp = []
            synonyms_tmp = []
            for i in ids:
                db_name = i['db_name']
                ranking_id = RANKING_ID.get(db_name, None)

                # only take the databases we define
                if ranking_id:
                    ids_tmp.append('{}/{}'.format(db_name, i['external_id']))
                    gene_lookup_data.append((ensembl_id, i['external_id'],
                                             ranking_id, species_id))
                    synonyms_tmp.extend(synonyms.get(i['xref_id'], []))

            if len(ids_tmp) > 0:
                ids_text = '||'.join(ids_tmp)

            if len(synonyms_tmp) > 0:
                synonyms_text = '||'.join(synonyms_tmp)

            for s in synonyms_tmp:
                gene_lookup_data.append((ensembl_id, s, 'GY', species_id))

        gene_data.append((ensembl_id, ensembl_id_version, species_id, symbol,
                          description, synonyms_text, ids_text, seq_id,
                          seq_start, seq_end, strand))

        gene_lookup_data.append((ensembl_id, ensembl_id, 'EG', species_id))
        gene_lookup_data.append((ensembl_id, symbol, 'GS', species_id))

        if description:
            gene_lookup_data.append((ensembl_id, description, 'GN', species_id))

        if counter and counter % 10000 == 0:
            cursor = conn.cursor()
            LOG.debug('Inserting {:,} genes...'.format(len(gene_data)))
            cursor.executemany(sql_genes_insert, gene_data)
            cursor.close()
            conn.commit()
            gene_data = []

            LOG.debug('Inserting {:,} '
                      'lookup records...'.format(len(gene_lookup_data)))
            cursor = conn.cursor()
            cursor.executemany(sql_genes_lookup_insert, gene_lookup_data)
            cursor.close()
            conn.commit()
            gene_lookup_data = []

    cursor = conn.cursor()
    LOG.debug('Inserting {:,} genes...'.format(len(gene_data)))
    cursor.executemany(sql_genes_insert, gene_data)
    cursor.close()
    conn.commit()

    LOG.debug('Inserting {:,} '
              'lookup records...'.format(len(gene_lookup_data)))
    cursor = conn.cursor()
    cursor.executemany(sql_genes_lookup_insert, gene_lookup_data)
    cursor.close()
    conn.commit()
    conn.close()

    LOG.info('Gene information inserted in: {}'.format(
        utils.format_time(start, time.time())))


def insert_gtpe(db, ref, gtep):
    """Insert the gene, transcript, protein, exon information into the database.

    Args:
        db (str): Name of the database file.

        ref (:obj:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

        gtep (list): A ``list`` of the gene, transcript, protein, exon
            information. Values were extracted via the following method:
            :func:`ensimpl.create.ensembl_db.extract_ensembl_gtpe`.
    """
    LOG.info('Inserting transcripts, proteins, exons '
             'into database: {}'.format(db))
    start = time.time()

    sql_gtpe_insert = 'INSERT INTO ensembl_gtpe_tmp ' + \
                      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ' + \
                      '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    attributes = ['gene_id', 'gene_version', 'gene_name', 'gene_chrom',
                  'gene_start', 'gene_end', 'gene_strand',
                  'transcript_id', 'transcript_version', 'transcript_name',
                  'transcript_start', 'transcript_end',
                  'protein_id', 'protein_version',
                  'exon_id', 'exon_version', 'exon_start', 'exon_end',
                  'exon_number']

    LOG.info('Generating transcript, protein, exon table...')
    conn = sqlite3.connect(db)

    gtpe_data = []
    species_id = ref.species_id

    for counter, g in enumerate(gtep):
        row = [g[attr] for attr in attributes]
        row.append(species_id)
        gtpe_data.append(tuple(row))

        if counter and counter % 50000 == 0:
            cursor = conn.cursor()
            LOG.debug('Inserting {:,} rows...'.format(len(gtpe_data)))
            cursor.executemany(sql_gtpe_insert, gtpe_data)
            cursor.close()
            conn.commit()
            gtpe_data = []

    LOG.debug('Inserting {:,} rows...'.format(len(gtpe_data)))
    cursor = conn.cursor()
    cursor.executemany(sql_gtpe_insert, gtpe_data)
    cursor.close()
    conn.commit()
    conn.close()

    LOG.info('Transcripts, proteins, exons inserted in: {}'.format(
        utils.format_time(start, time.time())))


def finalize(db, ref):
    """Finalize the database.  Move everything to where it needs to be and
    create the necessary indices.

     Args:
        db (str): Name of the database file.

        ref (:obj:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.
     """
    start = time.time()
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    LOG.info("Finalizing database....")

    sql_meta_insert = 'INSERT INTO meta_info VALUES (null, ?, ?, ?)'

    meta_data = []
    meta_data.append(('version', ref.version, ref.species_id))
    meta_data.append(('assembly', ref.assembly, ref.species_id))
    meta_data.append(('assembly_patch', ref.assembly_patch, ref.species_id))

    cursor.executemany(sql_meta_insert, meta_data)

    LOG.info('Finalizing chromosomes table...')
    cursor.execute(SQL_INSERT_CHROMOSOMES)
    conn.commit()

    LOG.info('Finalizing karyotypes table...')
    cursor.execute(SQL_INSERT_KARYOTYPES)
    conn.commit()

    LOG.info('Finalizing genes table...')
    cursor.execute(SQL_INSERT_GENES)
    conn.commit()

    LOG.info('Finalizing transcript, protein, exon table...')
    cursor.execute(SQL_INSERT_GTPE)
    conn.commit()

    LOG.info('Updating genes lookup...')
    cursor.execute(SQL_GENES_LOOKUP_TMP_INSERT)
    cursor.execute(SQL_GENES_LOOKUP_INSERT)
    conn.commit()

    LOG.info('Creating search table...')
    cursor.execute(SQL_ENSEMBL_SEARCH_INSERT)
    conn.commit()

    LOG.info('Creating indices...')
    for sql in SQL_INDICES:
        LOG.debug(sql)
        cursor.execute(sql)

    LOG.info('Cleaning up...')
    for sql in SQL_TABLES_DROP:
        LOG.debug(sql)
        cursor.execute(sql)

    conn.row_factory = sqlite3.Row

    LOG.info('Checking...')
    for sql in SQL_SELECT_CHECKS:
        LOG.debug(sql)
        cursor = conn.cursor()

        for row in cursor.execute(sql):
            LOG.info('**** WARNING ****')
            LOG.info(utils.dictify_row(cursor, row))
            break

        cursor.close()

    LOG.info('Information')
    for sql in SQL_SELECT_FINAL_INFO:
        LOG.debug(sql)
        cursor = conn.cursor()

        for row in cursor.execute(sql):
            LOG.info('{}\t{}\t{}'.format(row[0], row[1], row[2]))

        cursor.close()

    conn.execute('VACUUM')
    conn.commit()
    conn.close()

    LOG.info("Finalizing complete: {0}".format(
        utils.format_time(start, time.time())))


SQL_CREATE_TABLES = ['''
    CREATE TABLE IF NOT EXISTS meta_info (
       meta_info_key INTEGER,
       meta_key TEXT NOT NULL,
       meta_value TEXT NOT NULL,
       species_id TEXT NOT NULL,
       PRIMARY KEY (meta_info_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS search_ranking (
       search_ranking_key INTEGER,
       ranking_id TEXT,
       score INTEGER,
       description TEXT,
       PRIMARY KEY (search_ranking_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS chromosomes (
       chromosomes_key INTEGER,
       chromosome_num INTEGER NOT NULL,
       chromosome TEXT NOT NULL,
       chromosome_length INTEGER NOT NULL,
       species_id TEXT NOT NULL,
       PRIMARY KEY (chromosomes_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS chromosomes_tmp (
       chromosome_num INTEGER NOT NULL,
       chromosome TEXT NOT NULL,
       chromosome_length INTEGER NOT NULL,
       species_id TEXT NOT NULL
    );
''', '''
    CREATE TABLE IF NOT EXISTS karyotypes (
       karyotypes_key INTEGER,
       karyotype_num INTEGER NOT NULL,
       chromosome TEXT NOT NULL,
       seq_region_start INTEGER,
       seq_region_end INTEGER,
       band TEXT,
       stain TEXT,
       species_id TEXT NOT NULL,
       PRIMARY KEY (karyotypes_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS karyotypes_tmp (
       karyotype_num INTEGER NOT NULL,
       chromosome TEXT NOT NULL,
       seq_region_start INTEGER,
       seq_region_end INTEGER,
       band TEXT,
       stain TEXT,
       species_id TEXT NOT NULL
    );
''', '''
    CREATE TABLE IF NOT EXISTS ensembl_genes (
       ensembl_genes_key INTEGER,
       ensembl_id TEXT NOT NULL,
       ensembl_version TEXT,
       species_id TEXT NOT NULL,
       symbol TEXT,
       name TEXT,
       synonyms TEXT,
       external_ids TEXT,
       chromosome TEXT NOT NULL,
       start_position INTEGER NOT NULL,
       end_position INTEGER NOT NULL,
       strand INTEGER NOT NULL,
       PRIMARY KEY (ensembl_genes_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS ensembl_genes_tmp (
       ensembl_id TEXT NOT NULL,
       ensembl_version TEXT,
       species_id TEXT NOT NULL,
       symbol TEXT,
       name TEXT,
       synonyms TEXT,
       external_ids TEXT,
       chromosome TEXT NOT NULL,
       start_position INTEGER NOT NULL,
       end_position INTEGER NOT NULL,
       strand INTEGER NOT NULL
    );
''', '''
    CREATE TABLE IF NOT EXISTS ensembl_genes_lookup (
       ensembl_genes_lookup_key INTEGER,
       ensembl_gene_id TEXT,
       lookup_value TEXT COLLATE NOCASE,
       ranking_id TEXT,
       species_id TEXT,
       PRIMARY KEY (ensembl_genes_lookup_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS ensembl_genes_lookup_tmp (
       ensembl_gene_id TEXT,
       lookup_value TEXT,
       ranking_id TEXT,
       species_id TEXT
    );
''', '''
    CREATE TABLE IF NOT EXISTS ensembl_gtpe_tmp (
       gene_id TEXT,
       gene_version TEXT,
       gene_symbol TEXT,
       gene_chrom TEXT,
       gene_start INTEGER,
       gene_end INTEGER,
       gene_strand TEXT,
       transcript_id TEXT,
       transcript_version TEXT,
       transcript_symbol TEXT,
       transcript_start INTEGER,
       transcript_end INTEGER,
       protein_id TEXT,
       protein_version TEXT,       
       exon_id TEXT,
       exon_version TEXT,
       exon_start INTEGER,
       exon_end INTEGER,
       exon_number INTEGER,
       species_id TEXT
    );
''', '''
    CREATE TABLE IF NOT EXISTS ensembl_gtpe (
        gtpe_key INTEGER PRIMARY KEY,
        species_id TEXT NOT NULL,
        gene_id TEXT  NOT NULL,
        transcript_id TEXT,
        ensembl_id TEXT NOT NULL,
        ensembl_id_version INTEGER,
        ensembl_symbol TEXT,
        seqid TEXT NOT NULL,
        start INTEGER,
        end INTEGER,
        strand INTEGER,
        exon_number INTEGER,
        type_key TEXT NOT NULL
    )
''', '''
    CREATE VIRTUAL TABLE IF NOT EXISTS ensembl_search
        USING fts4(ensembl_genes_lookup_key, lookup_value);
''']

SQL_INSERT_CHROMOSOMES = '''
    INSERT
      INTO chromosomes
    SELECT null,
           chromosome_num,
           chromosome,
           chromosome_length,
           species_id
      FROM chromosomes_tmp
     ORDER BY species_id, chromosome_num;
'''

SQL_INSERT_KARYOTYPES = '''
    INSERT
      INTO karyotypes
    SELECT null,
           karyotype_num,
           chromosome,
           seq_region_start,
           seq_region_end,
           band,
           stain,
           species_id
      FROM karyotypes_tmp
     ORDER BY species_id, karyotype_num;
'''

SQL_INSERT_GENES = '''
    INSERT
      INTO ensembl_genes
    SELECT distinct null key,
           ensembl_id,
           ensembl_version,
           species_id,
           symbol,
           name,
           synonyms,
           external_ids,
           chromosome,
           start_position,
           end_position,
           strand
      FROM ensembl_genes_tmp
     ORDER BY species_id, ensembl_id;
'''

SQL_INSERT_GTPE = '''
    INSERT
      INTO ensembl_gtpe
    SELECT distinct null key,
           species_id,
           gene_id gene_id,
           null transcript_id,
           gene_id ensembl_id,
           gene_version,
           gene_symbol ensembl_symbol,
           gene_chrom,
           gene_start,
           gene_end,
           gene_strand,
           null exon_number,
           'EG' type_key
      FROM ensembl_gtpe_tmp
     UNION
    SELECT distinct null,
           species_id,
           gene_id,
           transcript_id,
           transcript_id,
           transcript_version,
           transcript_symbol,
           gene_chrom,
           transcript_start,
           transcript_end,
           gene_strand,
           null,
           'ET'
      FROM ensembl_gtpe_tmp
     UNION
    SELECT distinct null,
           species_id,
           gene_id,
           transcript_id,
           protein_id,
           protein_version,
           null,
           gene_chrom,
           transcript_start,
           transcript_end,
           gene_strand,
           null,
           'EP'
      FROM ensembl_gtpe_tmp
     WHERE protein_id IS NOT NULL
     UNION
    SELECT distinct null,
           species_id,
           gene_id,
           transcript_id,
           exon_id,
           exon_version,
           null,
           gene_chrom,
           exon_start,
           exon_end,
           gene_strand,
           exon_number,
           'EE'
      FROM ensembl_gtpe_tmp
     ORDER BY species_id, gene_id, transcript_id, ensembl_symbol desc, exon_number
'''

SQL_GENES_LOOKUP_TMP_INSERT = '''
    INSERT
      INTO ensembl_genes_lookup_tmp
    SELECT gene_id,
           transcript_id,
           type_key,
           species_id
      FROM ensembl_gtpe
     WHERE type_key = 'ET' 
     UNION
    SELECT gene_id,
           ensembl_symbol,
           'TS',
           species_id
      FROM ensembl_gtpe
     WHERE type_key = 'ET'
       AND ensembl_symbol IS NOT NULL
     UNION
    SELECT gene_id,
           ensembl_id,
           type_key,
           species_id
      FROM ensembl_gtpe
     WHERE type_key = 'EP' 
     UNION
    SELECT gene_id,
           ensembl_id,
           type_key,
           species_id
      FROM ensembl_gtpe
     WHERE type_key = 'EE' 
'''

SQL_GENES_LOOKUP_INSERT = '''
INSERT
  INTO ensembl_genes_lookup
SELECT distinct null,
       ensembl_gene_id,
       lookup_value,
       ranking_id,
       species_id TEXT
  FROM ensembl_genes_lookup_tmp
 WHERE lookup_value is not null
 ORDER BY species_id, ensembl_gene_id, lookup_value, ranking_id
'''

SQL_ENSEMBL_SEARCH_INSERT = '''
    INSERT
      INTO ensembl_search
    SELECT ensembl_genes_lookup_key, lookup_value
      FROM ensembl_genes_lookup
'''

SQL_INDICES = [
    'CREATE INDEX IF NOT EXISTS idx_gtpe_species_id ON ensembl_gtpe(species_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_gene_id ON ensembl_gtpe(gene_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_transcript_id ON ensembl_gtpe(transcript_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_ensembl_id ON ensembl_gtpe(ensembl_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_ensembl_id_version ON ensembl_gtpe(ensembl_id_version ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_seqid ON ensembl_gtpe(seqid ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_start ON ensembl_gtpe(start ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_end ON ensembl_gtpe(end ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_exon_number ON ensembl_gtpe(exon_number ASC)',
    'CREATE INDEX IF NOT EXISTS idx_gtpe_type_key ON ensembl_gtpe(type_key ASC)',
    'CREATE INDEX IF NOT EXISTS idx_ensembl_gene_id ON ensembl_genes (ensembl_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_lookup_ensembl_gene_id ON ensembl_genes_lookup (ensembl_gene_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_lookup_value ON ensembl_genes_lookup (lookup_value ASC)',
    'CREATE INDEX IF NOT EXISTS idx_lookup_id ON ensembl_genes_lookup (ranking_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_lookup_species_id ON ensembl_genes_lookup (species_id ASC)',
    'CREATE INDEX IF NOT EXISTS idx_chromosomes_cnum ON chromosomes (chromosome_num ASC);',
    'CREATE INDEX IF NOT EXISTS idx_chromosomes_chrom ON chromosomes (chromosome ASC);',
    'CREATE INDEX IF NOT EXISTS idx_chromosomes_species ON chromosomes (species_id ASC);',
    'CREATE INDEX IF NOT EXISTS idx_karyotypes_knum ON karyotypes (karyotype_num ASC);',
    'CREATE INDEX IF NOT EXISTS idx_karyotypes_chrom ON karyotypes (chromosome ASC);',
    'CREATE INDEX IF NOT EXISTS idx_karyotypes_species ON karyotypes (species_id ASC);',
    'CREATE INDEX IF NOT EXISTS idx_search_ranking_id ON search_ranking (ranking_id ASC);'
]

SQL_TABLES_DROP = [
    'DROP TABLE chromosomes_tmp',
    'DROP TABLE karyotypes_tmp',
    'DROP TABLE ensembl_genes_tmp',
    'DROP TABLE ensembl_genes_lookup_tmp',
    'DROP TABLE ensembl_gtpe_tmp'
]

SQL_TABLES_INITIALIZE = [
    'INSERT INTO search_ranking VALUES (null, "EE", 7000, "Ensembl Exon ID");',
    'INSERT INTO search_ranking VALUES (null, "EP", 6500, "Ensembl Protein ID");',
    'INSERT INTO search_ranking VALUES (null, "EG", 10000, "Ensembl Gene ID");',
    'INSERT INTO search_ranking VALUES (null, "ET", 7500, "Ensembl Transcript ID");',
    'INSERT INTO search_ranking VALUES (null, "GN", 5000, "Gene Name");',
    'INSERT INTO search_ranking VALUES (null, "GS", 6000, "Gene Symbol");',
    'INSERT INTO search_ranking VALUES (null, "GY", 5700, "Gene Synonym");',
    'INSERT INTO search_ranking VALUES (null, "TS", 5500, "Transcript Symbol");',
    'INSERT INTO search_ranking VALUES (null, "HG", 8300, "HGNC");',
    'INSERT INTO search_ranking VALUES (null, "MI", 8200, "MGI");',
    'INSERT INTO search_ranking VALUES (null, "UG", 8100, "UniGene");',
    'INSERT INTO search_ranking VALUES (null, "ZG", 8000, "EntrezGene");',
]

SQL_SELECT_FINAL_INFO = [
    '''
SELECT count(egl.lookup_value) num, sr.description, egl.species_id 
  FROM ensembl_genes_lookup egl, search_ranking sr
 WHERE egl.ranking_id = sr.ranking_id
 GROUP BY sr.description, egl.species_id 
 ORDER BY sr.score desc
   ''', '''
SELECT distinct meta_key meta_key, meta_value, species_id
  FROM meta_info
 ORDER BY meta_key       
    '''
]

SQL_SELECT_CHECKS = [
   '''
SELECT count(1), ensembl_id 
  FROM ensembl_genes 
 GROUP BY ensembl_id 
HAVING count(1) > 1
   ''', '''
SELECT *
  FROM ensembl_genes
 WHERE chromosome NOT IN (SELECT chromosome FROM chromosomes)
   ''', '''
SELECT distinct ensembl_id
  FROM ensembl_genes
EXCEPT
SELECT distinct gene_id
  FROM ensembl_gtpe
   '''
]
