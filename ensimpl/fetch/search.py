# -*- coding: utf_8 -*-

import sqlite3
import re

import ensimpl.utils as utils
import ensimpl.fetch.utils as fetch_utils

LOG = utils.get_logger()

REGEX_ENSEMBL_MOUSE_ID = re.compile("ENSMUS([EGTP])[0-9]{11}", re.IGNORECASE)
REGEX_ENSEMBL_HUMAN_ID = re.compile("ENS([EGTP])[0-9]{11}", re.IGNORECASE)
REGEX_MGI_ID = re.compile("MGI:[0-9]{1,}", re.IGNORECASE)
REGEX_REGION = re.compile("(CHR|)*\s*([0-9]{1,2}|X|Y|MT)\s*(-|:)?\s*(\d+)\s*(MB|M|K|)?\s*(-|:|)?\s*(\d+|)\s*(MB|M|K|)?", re.IGNORECASE)

SQL_TERM_EXACT = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND l.lookup_value = :term
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_TERM_EXACT_MM = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND l.lookup_value = :term
  AND l.species_id = 'mm'
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_TERM_EXACT_HS = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND l.lookup_value = :term
  AND l.species_id = 'hs'
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_TERM_LIKE = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       ensembl_search es,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND es.ensembl_genes_lookup_key = l.ensembl_genes_lookup_key
  AND es.lookup_value MATCH :term
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_TERM_LIKE_MM = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       ensembl_search es,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND es.ensembl_genes_lookup_key = l.ensembl_genes_lookup_key
  AND es.lookup_value MATCH :term
  AND l.species_id = 'mm'
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_TERM_LIKE_HS = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value)
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       ensembl_search es,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND es.ensembl_genes_lookup_key = l.ensembl_genes_lookup_key
  AND es.lookup_value MATCH :term
  AND l.species_id = 'hs'
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_ID = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       ensembl_search es,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND es.ensembl_genes_lookup_key = l.ensembl_genes_lookup_key
  AND l.ranking_id in ('EG', 'ET', 'EE', 'EP', 'ZG', 'MI', 'UG', 'HG')
  AND es.lookup_value MATCH :term
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_ID_MM = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       ensembl_search es,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND es.ensembl_genes_lookup_key = l.ensembl_genes_lookup_key
  AND l.species_id = 'mm'
  AND l.ranking_id in ('EG', 'ET', 'EE', 'EP', 'ZG', 'MI', 'UG', 'HG')
  AND es.lookup_value MATCH :term
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_ID_HS = '''
SELECT MAX(s.score||'||'||s.description||'||'||l.lookup_value) 
       AS match_description, l.ensembl_gene_id, g.*
  FROM ensembl_genes g,
       ensembl_genes_lookup l,
       ensembl_search es,
       search_ranking s
WHERE g.ensembl_id = l.ensembl_gene_id
  AND l.ranking_id = s.ranking_id
  AND es.ensembl_genes_lookup_key = l.ensembl_genes_lookup_key
  AND l.species_id = 'hs'
  AND l.ranking_id in ('EG', 'ET', 'EE', 'EP', 'ZG', 'MI', 'UG', 'HG')
  AND es.lookup_value MATCH :term
GROUP BY l.ensembl_gene_id
ORDER BY match_description - length(match_description) DESC, g.symbol ASC
'''

SQL_REGION = '''
SELECT *
  FROM ensembl_genes e
 WHERE e.chromosome = :chromosome
   AND e.start_position <= :end_position
   AND e.end_position >= :start_position
 ORDER BY cast(
       replace(replace(replace(e.chromosome,'X','50'),'Y','51'),'MT','51') 
       AS int), e.start_position, e.end_position
'''

SQL_REGION_MM = '''
SELECT *
  FROM ensembl_genes e
 WHERE e.chromosome = :chromosome
   AND e.start_position <= :end_position
   AND e.end_position >= :start_position
   AND e.species_id = 'mm'
 ORDER BY cast(
       replace(replace(replace(e.chromosome,'X','50'),'Y','51'),'MT','51') 
       AS int), e.start_position, e.end_position
'''

SQL_REGION_HS = '''
SELECT *
  FROM ensembl_genes e
 WHERE e.chromosome = :chromosome
   AND e.start_position <= :end_position
   AND e.end_position >= :start_position
   AND e.species_id = 'hs'
 ORDER BY cast(
       replace(replace(replace(e.chromosome,'X','50'),'Y','51'),'MT','51') 
       AS int), e.start_position, e.end_position
'''


QUERIES = {}
QUERIES['SQL_TERM_EXACT'] = SQL_TERM_EXACT
QUERIES['SQL_TERM_EXACT_MM'] = SQL_TERM_EXACT_MM
QUERIES['SQL_TERM_EXACT_HS'] = SQL_TERM_EXACT_HS
QUERIES['SQL_TERM_LIKE'] = SQL_TERM_LIKE
QUERIES['SQL_TERM_LIKE_MM'] = SQL_TERM_LIKE_MM
QUERIES['SQL_TERM_LIKE_HS'] = SQL_TERM_LIKE_HS
QUERIES['SQL_ID'] = SQL_ID
QUERIES['SQL_ID_MM'] = SQL_ID_MM
QUERIES['SQL_ID_HS'] = SQL_ID_HS
QUERIES['SQL_REGION'] = SQL_REGION
QUERIES['SQL_REGION_MM'] = SQL_REGION_MM
QUERIES['SQL_REGION_HS'] = SQL_REGION_HS


class SearchException(Exception):
    """Search exception class."""
    pass


class Query:
    """Encapsulate query objects.
    """
    def __init__(self, term=None, species_id=None, exact=False):
        """Initialization.

        Args:
            term (str): the query term
            species_id (str): the species id
            exact (bool): True for exact match
        """
        self.term = term
        self.species_id = species_id
        self.exact = exact
        self.region = None
        self.query = None

    def __str__(self):
        """Return a string representation of this query"""
        return 'Query: {}\nTerm: {}\nSpecies: {}\n' + \
               'Exact: {}\nRegion: {}'.format(self.query, self.term,
                                              self.species_id, self.exact,
                                              self.region)

    def get_paramaters(self):
        """Get the query parameters.

        Returns:
            dict: if region, than keys are 'chromosome', 'start_position',
                'end_position'. Otherwise, the key is 'term'
        """
        if self.region:
            return {'chromosome': self.region.chromosome,
                    'start_position': self.region.start_position,
                    'end_position': self.region.end_position}
        return {'term': self.term}


class Match:
    """Represent a match object.
    """
    def __init__(self, ensembl_gene_id=None, ensembl_version=None,
                 external_ids=None, symbol=None, name=None, synonyms=None,
                 species_id=None,  chromosome=None, position_start=None,
                 position_end=None, strand=None, match_reason=None,
                 match_value=None):
        self.ensembl_gene_id = ensembl_gene_id
        self.ensembl_version = ensembl_version
        self.external_ids = external_ids
        self.species_id = species_id
        self.symbol = symbol
        self.name = name
        self.synonyms = synonyms
        self.chromosome = chromosome
        self.position_start = position_start
        self.position_end = position_end
        self.strand = strand
        self.match_reason = match_reason
        self.match_value = match_value

    def __str__(self):
        return str(self.ensembl_gene_id)

    def dict(self):
        return self.__dict__


class Result:
    """
    Simple class to encapsulate a Query and matches
    """
    def __init__(self, query=None, matches=None, num_results=None):
        self.query = query
        self.matches = matches
        self.num_matches = len(matches) if matches else 0
        self.num_results = num_results


def get_query(term, species_id=None, exact=True):
    """Get query based upon parameters

    Args:
        term: the query object
        species_id: either 'Hs', 'Mm', or None
        exact: True for exact matches

    Returns:
        query: the query

    Raises:
        ValueError: when ``term`` is invalid
    """
    if not term:
        raise ValueError('No term')

    valid_term = term.strip()

    if len(valid_term) <= 0:
        raise ValueError('Empty term')

    if species_id and species_id.lower() not in ('hs', 'mm'):
        raise ValueError('Invalid species')

    query = Query(term, species_id, exact)

    if species_id:
        species_id = '_' + species_id.upper()
    else:
        species_id = ''

    if REGEX_ENSEMBL_MOUSE_ID.match(valid_term):
        query.query = QUERIES['SQL_ID' + species_id]
    elif REGEX_ENSEMBL_HUMAN_ID.match(valid_term):
        query.query = QUERIES['SQL_ID' + species_id]
    elif REGEX_MGI_ID.match(valid_term):
        query.query = QUERIES['SQL_ID' + species_id]
    elif REGEX_REGION.match(valid_term):
        query.query = QUERIES['SQL_REGION' + species_id]
        query.region = fetch_utils.str_to_region(term)
    else:
        if exact:
            query.query = QUERIES['SQL_TERM_EXACT' + species_id]
        else:
            query.query = QUERIES['SQL_TERM_LIKE' + species_id]

            if valid_term[-1] != '*':
                valid_term = valid_term + '*'

            query.term = valid_term

    return query


def execute_query(query, version=None, limit=None):
    """Execute the SQL query.

    Args:
        query (:obj:`Query`): the query
        version (int): database version
        limit (int): maximum number of items to return

    Returns:
        :obj:`Result`: the resulting object

    Raises:
        SearchException: when sqlite error or other error occurs
    """
    if not query:
        raise ValueError('No query')

    matches = []
    ilimit = fetch_utils.nvli(limit, -1)

    try:
        conn = fetch_utils.connect_to_database(version)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        for row in cursor.execute(query.query, query.get_paramaters()):
            match = Match()
            match.ensembl_gene_id = row['ensembl_gene_id']
            match.ensembl_version = row['ensembl_version']
            match.species_id = row['species_id']
            match.symbol = row['symbol']
            match.name = row['name']

            row_external_ids = row['external_ids']
            external_ids = []
            if row_external_ids:
                tmp_external_ids = row_external_ids.split('||')
                for e in tmp_external_ids:
                    elem = e.split('/')
                    external_ids.append({'db': elem[0], 'db_id': elem[1]})
            match.external_ids = external_ids

            row_synonyms = row['synonyms']
            synonyms = []
            if row_synonyms:
                synonyms = row_synonyms.split('||')

            match.synonyms = synonyms
            match.chromosome = row['chromosome']
            match.position_start = row['start_position']
            match.position_end = row['end_position']
            match.strand = '+' if row['strand'] > 0 else '-'

            if query.region:
                match.match_reason = 'Region'
                match.match_value = '{}:{}-{}'.format(str(match.chromosome),
                                                      str(match.position_start),
                                                      str(match.position_end))
            else:
                row_match_description = row['match_description']
                if row_match_description:
                    desc = row_match_description.split('||')
                match.match_reason = desc[1]
                match.match_value = desc[2]

            matches.append(match)

        cursor.close()
        conn.close()
    except sqlite3.Error as e:
        LOG.error('Database Error: {}'.format(e))
        raise SearchException(e)
    except Exception as e:
        LOG.error('Search Error: {}'.format(e))
        raise SearchException(e)

    num_matches = len(matches)

    if limit and len(matches) > ilimit:
        matches = matches[:ilimit]

    return Result(query, matches, num_matches)


def search(term, version=None, species_id=None, exact=True, limit=None):
    """Perform the search.

    Args:
        term (str): the term to look for
        version (int): Ensembl version or None for latest
        species_id (str): 'hs' or 'mm'
        exact (bool): True for exact match
        limit (int): number to retrun, None for all

    Returns:
        :obj:`Result`: the result of the query
    """
    LOG.debug('term={}'.format(term))
    LOG.debug('version={}'.format(version))
    LOG.debug('species_id={}'.format(species_id))
    LOG.debug('exact={}'.format(exact))
    LOG.debug('limit={}'.format(limit))

    try:
        query = get_query(term, species_id, exact)

        LOG.debug('QUERY={}'.format(query))

        result = execute_query(query, version, limit)

        LOG.debug('# matches: {}'.format(len(result.matches)))

        return result
    except SearchException as se:
        LOG.error('Error: {}'.format(se))
        return None