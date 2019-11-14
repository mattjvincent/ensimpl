# -*- coding: utf-8 -*-
"""This module is specific to Ensembl db operations.
"""
import pymysql

import ensimpl.utils as utils


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
SELECT DISTINCT sr.name, sr.length, k.seq_region_start, k.seq_region_end, 
       k.band, k.stain
  FROM coord_system cs,
       seq_region sr
  LEFT JOIN karyotype k ON k.seq_region_id = sr.seq_region_id
 WHERE cs.coord_system_id = sr.coord_system_id
   AND sr.name in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 
                   '11',  '12', '13', '14', '15', '16', '17', '18', '19', '20',
                   '21', '22', 'X', 'Y', 'MT')
   AND cs.rank = 1
 ORDER BY cast(replace(replace(replace(sr.name, 'X', '50'), 'Y', '51'), 
          'MT', '52') as signed), k.seq_region_start
'''

SQL_ENSEMBL_COMPARA_75_SELECT_HOMOLOGS = '''
SELECT h.homology_id, 
       h.description,
       h.dn,
       h.ds,
       null goc_score,
       null wga_coverage,
       null is_high_confidence,
       gm_hs.stable_id hs_id,
       gm_hs.version hs_version,
       gm_hs.display_label hs_symbol, 
       gm_mm.stable_id mm_id,
       gm_mm.version mm_version,
       gm_mm.display_label mm_symbol, 
       hm_hs.perc_cov hs_perc_cov,
       hm_hs.perc_id hs_perc_id,
       hm_hs.perc_pos hs_perc_pos,            
       hm_mm.perc_cov mm_perc_cov,
       hm_mm.perc_id mm_perc_id,
       hm_mm.perc_pos mm_perc_pos            
  FROM homology h,
       homology_member hm_mm,
       member gm_mm,
       homology_member hm_hs,
       member gm_hs
 WHERE h.homology_id = hm_mm.homology_id
   AND hm_mm.member_id = gm_mm.member_id
   AND gm_mm.taxon_id = 10090
   AND h.homology_id = hm_hs.homology_id
   AND hm_hs.member_id  = gm_hs.member_id
   AND gm_hs.taxon_id = 9606
   AND h.method_link_species_set_id = (
       SELECT DISTINCT ml.method_link_species_set_id
         FROM species_set s,
              method_link_species_set ml,
              method_link m,
              (  
               SELECT s.species_set_id 
                 FROM species_set s,
                      genome_db g
                WHERE s.genome_db_id = g.genome_db_id
                  AND g.taxon_id = 9606
              ) homosapiens,
              (
               SELECT s.species_set_id 
                 FROM species_set s,
                      genome_db g
                WHERE s.genome_db_id = g.genome_db_id
                  AND g.taxon_id = 10090
              ) musmusculus
        WHERE homosapiens.species_set_id = musmusculus.species_set_id
          AND musmusculus.species_set_id = s.species_set_id
          AND s.species_set_id =  ml.species_set_id
          AND ml.method_link_id = m.method_link_id
          AND ml.source != 'NULL'
          AND m.type = 'ENSEMBL_ORTHOLOGUES')                  
'''


SQL_ENSEMBL_COMPARA_76_85_SELECT_HOMOLOGS = '''
SELECT h.homology_id, 
       h.description,
       h.dn,
       h.ds,
       null goc_score,
       null wga_coverage,
       null is_high_confidence,
       gm_hs.stable_id hs_id,
       gm_hs.version hs_version,
       gm_hs.display_label hs_symbol, 
       gm_mm.stable_id mm_id,
       gm_mm.version mm_version,
       gm_mm.display_label mm_symbol, 
       hm_hs.perc_cov hs_perc_cov,
       hm_hs.perc_id hs_perc_id,
       hm_hs.perc_pos hs_perc_pos,            
       hm_mm.perc_cov mm_perc_cov,
       hm_mm.perc_id mm_perc_id,
       hm_mm.perc_pos mm_perc_pos            
  FROM homology h,
       homology_member hm_mm,
       gene_member gm_mm,
       homology_member hm_hs,
       gene_member gm_hs
 WHERE h.homology_id = hm_mm.homology_id
   AND hm_mm.gene_member_id = gm_mm.gene_member_id
   AND gm_mm.taxon_id = 10090
   AND h.homology_id = hm_hs.homology_id
   AND hm_hs.gene_member_id  = gm_hs.gene_member_id
   AND gm_hs.taxon_id = 9606
   AND h.method_link_species_set_id = (
       SELECT DISTINCT ml.method_link_species_set_id
         FROM species_set s,
              method_link_species_set ml,
              method_link m,
              (  
               SELECT s.species_set_id 
                 FROM species_set s,
                      genome_db g
                WHERE s.genome_db_id = g.genome_db_id
                  AND g.taxon_id = 9606
              ) homosapiens,
              (
               SELECT s.species_set_id 
                 FROM species_set s,
                      genome_db g
                WHERE s.genome_db_id = g.genome_db_id
                  AND g.taxon_id = 10090
              ) musmusculus
        WHERE homosapiens.species_set_id = musmusculus.species_set_id
          AND musmusculus.species_set_id = s.species_set_id
          AND s.species_set_id =  ml.species_set_id
          AND ml.method_link_id = m.method_link_id
          AND ml.source != 'NULL'
          AND m.type = 'ENSEMBL_ORTHOLOGUES')     
'''


SQL_ENSEMBL_COMPARA_86_SELECT_HOMOLOGS = '''
SELECT h.homology_id, 
       h.description,
       h.dn,
       h.ds,
       h.goc_score,
       h.wga_coverage,
       h.is_high_confidence,
       gm_hs.stable_id hs_id,
       gm_hs.version hs_version,
       gm_hs.display_label hs_symbol, 
       gm_mm.stable_id mm_id,
       gm_mm.version mm_version,
       gm_mm.display_label mm_symbol, 
       hm_hs.perc_cov hs_perc_cov,
       hm_hs.perc_id hs_perc_id,
       hm_hs.perc_pos hs_perc_pos,            
       hm_mm.perc_cov mm_perc_cov,
       hm_mm.perc_id mm_perc_id,
       hm_mm.perc_pos mm_perc_pos            
  FROM homology h,
       homology_member hm_mm,
       gene_member gm_mm,
       homology_member hm_hs,
       gene_member gm_hs
 WHERE h.homology_id = hm_mm.homology_id
   AND hm_mm.gene_member_id = gm_mm.gene_member_id
   AND gm_mm.taxon_id = 10090
   AND h.homology_id = hm_hs.homology_id
   AND hm_hs.gene_member_id  = gm_hs.gene_member_id
   AND gm_hs.taxon_id = 9606
   AND h.method_link_species_set_id = (
       SELECT DISTINCT ml.method_link_species_set_id
         FROM species_set s,
              method_link_species_set ml,
              method_link m,
              (  
               SELECT s.species_set_id 
                 FROM species_set s,
                      genome_db g
                WHERE s.genome_db_id = g.genome_db_id
                  AND g.taxon_id = 9606
              ) homosapiens,
              (
               SELECT s.species_set_id 
                 FROM species_set s,
                      genome_db g
                WHERE s.genome_db_id = g.genome_db_id
                  AND g.taxon_id = 10090
              ) musmusculus
        WHERE homosapiens.species_set_id = musmusculus.species_set_id
          AND musmusculus.species_set_id = s.species_set_id
          AND s.species_set_id =  ml.species_set_id
          AND ml.method_link_id = m.method_link_id
          AND ml.source != 'NULL'
          AND m.type = 'ENSEMBL_ORTHOLOGUES')     
'''

SQL_ENSEMBL_COMPARA_SELECT_HOMOLOGS_MM_ORDER_BY = '''
   ORDER BY mm_id, hs_id
'''

SQL_ENSEMBL_COMPARA_SELECT_HOMOLOGS_HS_ORDER_BY = '''
   ORDER BY hs_id, mm_id
'''


def _get_sql_select_gene(release):
    """Retrieve the proper SQL statement based upon the Ensembl `release`.

    Args:
        release (int): Ensembl release.

    Returns:
        str: The SQL statement.
    """
    if int(release) < 65:
        return SQL_ENSEMBL_48_SELECT_GENE + SQL_ENSEMBL_48_SELECT_GENE_ORDER_BY
    else:
        return SQL_ENSEMBL_65_SELECT_GENE + SQL_ENSEMBL_65_SELECT_GENE_ORDER_BY


def _get_sql_select_gtpe(release):
    """Retrieve the proper SQL statement based upon the Ensembl `release`.

    Args:
        release (int): Ensembl release.

    Returns:
        str: The SQL statement.
    """
    if int(release) < 65:
        return SQL_ENSEMBL_48_SELECT_GTPE + SQL_ENSEMBL_48_SELECT_GTPE_ORDER_BY
    else:
        return SQL_ENSEMBL_65_SELECT_GTPE + SQL_ENSEMBL_65_SELECT_GTPE_ORDER_BY


def _get_sql_select_gene_homologs(release):
    """Retrieve the proper SQL statement based upon the Ensembl `release`.

    Args:
        release (int): Ensembl release.

    Returns:
        str: The SQL statement.
    """
    if int(release) <= 75:
        return SQL_ENSEMBL_COMPARA_75_SELECT_HOMOLOGS
    elif int(release) <= 85:
        return SQL_ENSEMBL_COMPARA_76_85_SELECT_HOMOLOGS
    else:
        return SQL_ENSEMBL_COMPARA_86_SELECT_HOMOLOGS


def connect_to_database(ref, db=None):
    """Connect to Ensembl database.

    Args:
        ref (:class:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.
        db:

    Returns:
        :class:`pymysql.connections.Connection`: A connection to the database.
    """
    try:
        LOG.debug('Connecting to {} ...'.format(ref.server))

        if not db:
            db = ref.db

        connection = pymysql.connect(host=ref.server,
                                     port=int(ref.port),
                                     user=ref.user_id,
                                     password=ref.password,
                                     db=db,
                                     cursorclass=pymysql.cursors.DictCursor)

        LOG.debug('Connected')
        return connection
    except pymysql.Error as e:
        LOG.error('Unable to connect to Ensembl: {}'.format(e))
        raise e


def extract_chromosomes_karyotypes(ref):
    """Extract the chromosomes and karyotypes from Ensembl.

    Each ``dict`` in the ``list`` has the following keys:

    * ``name`` - chromosome name
    * ``length`` - chromosome length
    * ``seq_region_start`` - karyotype start
    * ``seq_region_end`` - karyotype end
    * ``band`` - karyotype band
    * ``stain`` - karyotype stain

    Args:
        ref (:class:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

    Returns:
        list: A ``list`` of ``dicts``.
    """
    chromosomes = []

    try:
        conn = connect_to_database(ref)

        LOG.debug('Extracting chromosomes and karyotypes...')
        with conn.cursor() as cursor:
            num_rows = cursor.execute(SQL_ENSEMBL_SELECT_CHROMOSOME)
            LOG.debug(f'{num_rows:,} records returned')

            for row in cursor:
                chromosomes.append(row)

            LOG.debug(f'{num_rows:,} records extracted')
    except pymysql.Error as e:
        LOG.error(f'Unable to extract chromosomes from Ensembl: {e}')
        return None

    return chromosomes


def extract_synonyms(ref):
    """Extract the synonyms from Ensembl.

    Args:
        ref (:class:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

    Returns:
        dict: The keys are the xref_id and the values are a ``list`` of
            synonyms.
    """
    synonyms = {}

    try:
        conn = connect_to_database(ref)

        LOG.debug('Extracting synonyms...')
        with conn.cursor() as cursor:
            count = 0
            num_rows = cursor.execute(SQL_ENSEMBL_SELECT_SYNONYMS)
            LOG.debug(f'{num_rows:,} records returned')

            for row in cursor:
                xref = synonyms.get(row['xref_id'], None)

                if not xref:
                    xref = []

                xref.append(row['synonym'])

                synonyms[row['xref_id']] = xref

                if count and count % 10000 == 0:
                    LOG.debug(f'{count:,} synonyms extracted')
                count += 1

            LOG.debug(f'{count:,} synonyms extracted')
    except pymysql.Error as e:
        LOG.error(f'Unable to extract synonyms from Ensembl: {e}')
        return None

    return synonyms


def extract_ensembl_genes(ref):
    """Extract the gene information from Ensembl.

    Each gene returned is a ``dict`` with the following keys:

    * ``ensembl_id`` - Ensembl ID
    * ``ensembl_id_version`` - version of Ensembl ID
    * ``seq_id`` - chromosome
    * ``seq_region_start`` - start in base pairs
    * ``seq_region_end`` - end in base pairs
    * ``seq_region_strand`` - strand
    * ``symbol`` - Ensemble Gene symbol
    * ``description`` - description of gene
    * ``ids`` - ``dict`` of ids with the following keys:
        * ``xref_id`` - cross reference id from Ensembl
        * ``external_id`` - id of gene in external database
        * ``db_name`` - external database name

    Args:
        ref (:class:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

    Returns:
        dict: Gene information with the Ensembl ID being the key.
    """
    gene_items = ['ensembl_id', 'ensembl_id_version', 'seq_id',
                  'seq_region_start', 'seq_region_end', 'seq_region_strand',
                  'symbol', 'description']
    genes = {}

    try:
        conn = connect_to_database(ref)
        sql = _get_sql_select_gene(ref.release)

        LOG.debug('Extracting genes...')
        with conn.cursor() as cursor:
            num_rows = cursor.execute(sql)
            LOG.debug(f'{num_rows:,} records returned')

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

                if len(genes) and len(genes) % 10000 == 0:
                    LOG.debug(f'{len(genes):,} genes extracted')

            LOG.debug(f'{len(genes):,} genes extracted')
    except pymysql.Error as e:
        LOG.error(f'Unable to extract genes from Ensembl: {e}')
        return None

    return genes


def extract_ensembl_gtpe(ref):
    """Extract the gene, transcript, protein, exon information from Ensembl.

    Args:
        ref (:class:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

    Returns:
        list: A ``list`` of the gene, transcript, protein, exon information.
           Look at :data:`SQL_ENSEMBL_65_SELECT_GTPE` and
           :data:`SQL_ENSEMBL_48_SELECT_GTPE` for the information extracted.
    """
    gtep = []
    genes = {}

    try:
        conn = connect_to_database(ref)
        sql = _get_sql_select_gtpe(ref.release)

        LOG.debug('Extracting transcript, protein, exon information ...')
        with conn.cursor() as cursor:
            count = 0
            num_rows = cursor.execute(sql)
            LOG.debug(f'{num_rows:,} records returned')

            for row in cursor:
                gtep.append(row)
                genes[row['gene_id']] = 1

                if len(gtep) and len(gtep) % 100000 == 0:
                    LOG.debug(f'{len(gtep):,} records extracted')
                count += 1

            LOG.debug(f'{len(gtep):,} records extracted')
    except pymysql.Error as e:
        LOG.error('Unable to extract transcript, protein, exon from '
                  f'Ensembl: {e}')
        return None

    return gtep


def extract_ensembl_homologs(ref):
    """Extract the homologs from Ensembl.

    Args:
        ref (:class:`ensimpl.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.

    Returns:
        dict: A ``dict`` of genes and there homologs.
    """
    homologs = {}

    try:
        conn = connect_to_database(ref, ref.compara_db)
        sql = _get_sql_select_gene_homologs(ref.release)

        if ref.species_id.lower() == 'hs':
            e_id = 'hs_id'
            sql += SQL_ENSEMBL_COMPARA_SELECT_HOMOLOGS_HS_ORDER_BY
        else:
            e_id = 'mm_id'
            sql += SQL_ENSEMBL_COMPARA_SELECT_HOMOLOGS_MM_ORDER_BY

        LOG.debug('Extracting homologs ...')
        with conn.cursor() as cursor:
            count = 0
            num_rows = cursor.execute(sql)
            LOG.debug(f'{num_rows:,} records returned')

            for row in cursor:
                if row[e_id] not in homologs:
                    homologs[row[e_id]] = [row]
                else:
                    homologs[row[e_id]].append(row)

                if count and count % 10000 == 0:
                    LOG.debug(f'{count:,} homologs extracted')
                count += 1

            LOG.debug(f'{count:,} homologs extracted')

    except pymysql.Error as e:
        LOG.error(f'Unable to extract homologs from Ensembl: {e}')
        return None

    return homologs



