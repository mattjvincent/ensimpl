# -*- coding: utf-8 -*-
from functools import wraps

from flask import Blueprint
from flask import current_app
from flask import jsonify
from flask import render_template
from flask import request

import ensimpl.db_config as db_config
import ensimpl.utils as ensimpl_utils

from ensimpl.fetch import get
from ensimpl.fetch import genes as genes_ensimpl
from ensimpl.fetch import history as genes_history
from ensimpl.fetch import search as search_ensimpl
from ensimpl.fetch import utils as fetch_utils

api = Blueprint('api', __name__, template_folder='templates', url_prefix='/api')


def support_jsonp(func):
    """Wraps JSONified output for JSONP requests."""

    @wraps(func)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            resp = func(*args, **kwargs)
            resp.set_data('{}({})'.format(str(callback),
                                          resp.get_data(as_text=True)))
            resp.mimetype = 'application/javascript'
            return resp
        else:
            return func(*args, **kwargs)

    return decorated_function



'''

JSONP - GET only

GET /api/releases

GET /api/stats?release=<:string>&species=<:string>

GET /api/chromosomes?release=<:string>&species=<:string>

GET /api/karyotypes?release=<:string>&species=<:string>

GET /api/extdbs?release=<:string>&species=<:string>

POST /api/extids

    -- source_ids:list of string
    -- source_db:string
    -- release:string
    -- species:string

-- get single gene information 
GET /api/gene/<ensembl_id:string>?release=<:string>&species=<:string>&details=<:string>

-- get batch gene information
POST /api/genes

    -- ensembl_ids:list of string
    -- release:string
    -- species:string
    -- details:string

-- get single gene history information 
GET /api/gene_history/<ensembl_id:string>?release=<:string>&species=<:string>&details=<:string>


GET /api/search/gene?q=<:string>&limit=<:number>&release=<:string>&species=<:string>&details=<:string>

-- get single gene history information 
GET /api/random_ids/<source_db:string>?release=<:string>&species=<:string>&details=<:string>


'''

@api.route("/releases", methods=['GET'])
@support_jsonp
def releases():
    """Get all the release and species information.

    No parameters are needed.

    If successful, a JSON response will be returned with a single
    ``release`` element containing a ``list`` of releases consisting of the
    following items:

    ==============  =======  ==================================================
    Param           Type     Description
    ==============  =======  ==================================================
    release         integer  the Ensembl release
    species         string   the species identifier (example 'Hs', 'Mm')
    assembly        string   the genome assembly information
    assembly_patch  string   the genome assembly patch number
    ==============  =======  ==================================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    release_info = []

    try:
        for dbs in db_config.ENSIMPL_DBS:
            release_info.append(get.db_meta(dbs['release'], dbs['species']))
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify({'releases': release_info})


@api.route("/stats", methods=['GET'])
@support_jsonp
def stats():
    """Get the information for a particular Ensembl release and species.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    release  string   the Ensembl release
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``release`` element containing a ``list`` of releases consisting of the
    following items:

    ==============  =======  ==================================================
    Param           Type     Description
    ==============  =======  ==================================================
    release         string   the Ensembl release
    species         string   the species identifier (example 'Hs', 'Mm')
    assembly        string   the genome assembly information
    assembly_patch  string   the genome assembly patch number
    stats           dict     various stats about the database
    ==============  =======  ==================================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    release = request.values.get('release', None)
    species = request.values.get('species', None)

    ret = {}

    try:
        ret['meta'] = get.db_meta(release, species)
        ret['stats'] = get.stats(release, species)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/chromosomes", methods=['GET'])
@support_jsonp
def chromosomes():
    """Get the chromosome information.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    release  string   the Ensembl release
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``chromosomes`` element containing a ``list`` of chromosomes consisting
    of the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    chromosome         string   chromosome
    order              integer  chromosome order in the genome
    length             int      length of chromosome in base pairs
    =================  =======  ============================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    release = request.values.get('release', None)
    species = request.values.get('species', None)

    ret = {}

    try:
        ret['meta'] = get.db_meta(release, species)
        ret['chromosomes'] = get.chromosomes(release, species)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/karyotypes", methods=['GET'])
@support_jsonp
def karyotypes():
    """Get the karyotype information.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    release  string   the Ensembl release
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``chromosomes`` element containing a ``list`` of chromosomes consisting
    of the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    chromosome         string   chromosome
    order              integer  chromosome order in the genome
    length             int      length of chromosome in base pairs
    karyotypes         list     each element being a ``karyotype_element``
    =================  =======  ============================================

    A ``karyotype_element`` contains the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    band               string   name of the band
    seq_region_end     integer  start position
    seq_region_start   integer  end position
    stain              string   name of the stain
    =================  =======  ============================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    release = request.values.get('release', None)
    species = request.values.get('species', None)

    ret = {}

    try:
        ret['meta'] = get.db_meta(release, species)
        ret['chromosomes'] = get.karyotypes(release, species)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/external_dbs", methods=['GET'])
@support_jsonp
def external_dbs():
    """Get the external database information.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    release  string   the Ensembl release
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``external_dbs`` element containing a ``list`` of external databases
    consisting of the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    external_db_id     string   unique external db identifier
    external_db_name   string   external db name
    ranking_id         string   internal ranking id
    =================  =======  ============================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    release = request.values.get('release', None)
    species = request.values.get('species', None)

    ret = {}

    try:
        ret['meta'] = get.db_meta(release, species)
        ret['external_dbs'] = get.external_dbs(release, species)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/gene/<ensembl_id>", methods=['GET'])
@support_jsonp
def gene(ensembl_id):
    """Get the information for an Ensembl gene.

    The following is a list of the valid query parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    release  string   the Ensembl release
    species  string   the species identifier (example 'Hs', 'Mm')
    details  string   true, false, T, F, 0, 1
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single ``gene``
    element consisting the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    id                 string   Ensembl gene identifier
    ensembl_version    integer  version of the identifier
    species_id         string   species identifier: 'Mm', 'Hs', etc
    chromosome         string   the chromosome
    start              integer  start position in base pairs
    end                integer  end position in base pairs
    strand             string   '+' or '-'
    name               string   name of the gene
    symbol             string   gene symbol
    synonyms           list     list of strings
    external_ids       list     each having keys of 'db' and 'db_id'
    homolog_ids        list     each having keys of 'homolog_id' and
                                'homolog_symbol'
    transcripts        list     each having a ``transcript`` element
    =================  =======  ============================================

    ``transcript_element``, with each item containing:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    id                 string   Ensembl transcript identifier
    ensembl_version    integer  version of the identifier
    symbol             string   transcript symbol
    start              integer  start position in base pairs
    end                integer  end position in base pairs
    exons              list     dict of: number,id,start,end,ensembl_version
    protein            dict     id, start, end, ensembl_version
    =================  =======  ============================================

    If the id is not found, the gene will still be returned but have
    ``null`` for a value.

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    release = request.values.get('release', None)
    species = request.values.get('species', None)
    details = ensimpl_utils.str2bool(request.values.get('details', '0'))

    try:
        ret = {'meta': get.db_meta(release, species),
               'gene': None}

        if not id:
            raise ValueError('No id specified')

        results = genes_ensimpl.get(ids=[ensembl_id],
                                    release=release,
                                    species=species,
                                    details=details)

        if len(results) == 0:
            current_app.logger.info(f'No results found for: {ensembl_id}')
            return jsonify(ret)

        if len(results) > 1:
            raise ValueError(f'Too many genes found for: {ensembl_id}')

        ret['gene'] = results
    except Exception as e:
        current_app.logger.error(str(e))
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/genes", methods=['GET', 'POST'])
@support_jsonp
def genes():
    """Get the information for an Ensembl gene.

    The following is a list of the valid parameters:

    ==========  =======  ===================================================
    Param       Type     Description
    ==========  =======  ===================================================
    ids         list     repeated id elements, one per Ensembl id
    release     integer  the Ensembl release
    species     string   the species identifier (example 'Hs', 'Mm')
    details     string   True for all information, False for high level
    ==========  =======  ===================================================

    If successful, a JSON response will be returned with multiple ``gene``
    elements, each consisting of the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    id                 string   Ensembl gene identifier
    ensembl_version    integer  version of the identifier
    species_id         string   species identifier: 'Mm', 'Hs', etc
    chromosome         string   the chromosome
    start              integer  start position in base pairs
    end                integer  end position in base pairs
    strand             string   '+' or '-'
    name               string   name of the gene
    symbol             string   gene symbol
    synonyms           list     list of strings
    external_ids       list     each having keys of 'db' and 'db_id'
    homolog_ids        list     each having keys of 'homolog_id' and
                                'homolog_symbol'
    =================  =======  ============================================

    If ``details`` is ``True``, each gene will also contain the following:

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

    and ``homologs``, with each item containing:

    ====================  =======  ============================================
    Element               Type     Description
    ====================  =======  ============================================
    ensembl_homologs_key  integer  unique identifier
    ensembl_id            string   Ensembl gene identifer
    =================  =======  ============================================


    If an id is not found, the gene will still be returned but have
    ``null`` for a value.

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    ids = None
    release = None
    species = None
    details = False

    if request.is_json:
        if 'release' in request.json:
            release = request.json['release']
        if 'species' in request.json:
            species = request.json['species']
        if 'ids[]' in request.json:
            ids = request.json['ids[]']
        if 'details' in request.json:
            details = ensimpl_utils.str2bool(request.json['details'])
    else:
        release = request.values.get('release', None)
        species = request.values.get('species', None)
        ids = request.values.getlist('ids[]', None)
        details = ensimpl_utils.str2bool(request.values.get('details', '0'))

    ret = {'meta': get.db_meta(release, species),
           'genes': None}

    try:
        results = genes_ensimpl.get(ids=ids,
                                    release=release,
                                    species=species,
                                    details=details)

        if len(results) == 0:
            current_app.logger.info('No results found')
            return jsonify(ret)

        ret['genes'] = results
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/external_ids", methods=['GET', 'POST'])
@support_jsonp
def external_ids():
    """Get the information for an Ensembl gene.

    The following is a list of the valid parameters:

    ========  =======  ===================================================
    Param     Type     Description
    ========  =======  ===================================================
    ids       list     repeated id elements, one per Ensembl id
    source_db string   Defaults to 'Ensembl', but other are valid, please see
                       external_dbs().
    release   string   the Ensembl release
    species   string   the species identifier (example 'Hs', 'Mm')
    ========  =======  ===================================================

    If successful, a JSON response will be returned with multiple ``gene``
    elements, each consisting of the following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    id                 string   Ensembl gene identifier
    ensembl_version    integer  version of the identifier
    =================  =======  ============================================


    If an id is not found, the gene will still be returned but have
    ``null`` for a value.

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    ids = None
    source_db = False
    release = None
    species = None

    if request.is_json:
        if 'ids[]' in request.json:
            ids = request.json['ids[]']
        if 'source_db' in request.json:
            source_db = request.json['source_db']
        if 'release' in request.json:
            release = request.json['release']
        if 'species' in request.json:
            species = request.json['species']
    else:
        ids = request.values.getlist('ids[]', None)
        source_db = request.values.get('source_db', None)
        release = request.values.get('release', None)
        species = request.values.get('species', None)

    ret = {'meta': get.db_meta(release, species),
           'ids': None}

    try:
        results = genes_ensimpl.get_ids(ids=ids,
                                        release=release,
                                        species=species,
                                        source_db=source_db)

        if len(results) == 0:
            current_app.logger.info('No results found')
            return jsonify(ret)

        ret['ids'] = results
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/search", methods=['GET'])
@support_jsonp
def search():
    """Perform a search of a Ensimpl database.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    term     string   the term to search for
    release  string   the Ensembl release
    species  string   the species identifier (example 'Hs', 'Mm')
    exact    string   to exact match or not, defaults to 'False'
    limit    string   max number of items to return, defaults to 100,000
    =======  =======  ===================================================

    If sucessful, a JSON response will be returned with the following elements:

    =======  =======  ===================================================
    Element  Type     Description
    =======  =======  ===================================================
    request  dict     the request parameters
    result   dict     the results
    =======  =======  ===================================================

    The ``request`` dictionary will have the same values as listed above in the
    valid parameters.

    The ``result`` dictionary will have the following elements:

    ============  =======  ===================================================
    Element       Type     Description
    ============  =======  ===================================================
    num_results   int      the total number of matches
    num_matches   int      the number of matches returned (limited by limit)
    matches       list     a list of match objects
    ============  =======  ===================================================

    Each match object will contain:

    ================  =======  ===============================================
    Element           Type     Description
    ================  =======  ===============================================
    match_reason      string   reason of the match: name, synonym, id, etc
    match_value       string   value that matched
    ensembl_gene_id   string   Ensembl gene identifier
    ensembl_version   integer  version of the identifier
    chromosome        string   the chromosome
    position_start    integer  start position in base pairs
    position_end      integer  end position in base pairs
    strand            string   '+' or '-'
    species           string   species identifier: 'Mm', 'Hs', etc
    name              string   name of the gene
    symbol            string   gene symbol
    synonyms          list     list of strings
    external_ids      list     each having keys of 'db' and 'db_id
    ================  =======  ===============================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    term = request.values.get('term', None)
    release = request.values.get('release', None)
    species = request.values.get('species', None)
    exact = ensimpl_utils.str2bool(request.values.get('exact', '0'))
    limit = request.values.get('limit', '100000')

    try:
        limit = int(limit)
    except ValueError as ve:
        limit = 100000
        current_app.logger.info(ve)

    request_params = {'term': term, 'species': species, 'exact': exact,
                      'limit': limit, 'release': release}

    current_app.logger.debug(f'PARAMS: {request_params}')

    ret = {'meta': get.db_meta(release, species),
           'request': request_params,
           'result': {'num_results': 0,
                      'num_matches': 0,
                      'matches': None}}

    try:
        results = search_ensimpl.search(term=term,
                                        release=release,
                                        species=species,
                                        exact=exact,
                                        limit=limit)

        if len(results.matches) == 0:
            current_app.logger.info('No results found')
            return jsonify(ret)

        ret['result']['num_results'] = results.num_results
        ret['result']['num_matches'] = results.num_matches
        ret['result']['matches'] = []

        for match in results.matches:
            ret['result']['matches'].append(match.dict())

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/history", methods=['GET'])
@support_jsonp
def history():
    """Perform a search of an Ensimpl Identifier.

    The following is a list of the valid parameters:

    id, species, version_start=None, version_end=None, full=False):

    =============  =======  ===================================================
    Param          Type     Description
    =============  =======  ===================================================
    id             string   the Ensembl id
    species        string   the species identifier (example 'Hs', 'Mm')
    version_start  integer  the start version to search for
    version_end    integer  the end version to search for
    =============  =======  ===================================================

    If sucessful, a JSON response will be returned with the following elements:


    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    ensembl_id = None
    species = None
    release_start = None
    release_end = None

    if request.is_json:
        if 'ensembl_id' in request.json:
            ensembl_id = request.json['ensembl_id']
        if 'species' in request.json:
            species = request.json['species']
        if 'release_start' in request.json:
            release_start = fetch_utils.nvli(request.json['release_start'],
                                             None)
        if 'release_end' in request.json:
            release_end = fetch_utils.nvli(request.json['release_end'],
                                           None)
    else:
        ensembl_id = request.values.get('id', None)
        species = request.values.get('species', None)
        release_start = request.values.get('release_start', None)
        release_end = request.values.get('release_end', None)

    request_params = {'ensembl_id': ensembl_id,
                      'species': species,
                      'release_start': release_start,
                      'release_end': release_end}

    current_app.logger.debug('PARAMS: {}'.format(request_params))

    ret = {'request': request_params,
           'history': None}

    try:
        results = genes_history.get_history(ensembl_id,
                                            release_start,
                                            release_end,
                                            species)

        if len(results) == 0:
            current_app.logger.info('No results found')
            return jsonify(ret)

        ret['history'] = results

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/randomids")
@support_jsonp
def random_ids():
    """Get random ids.  Mostly useful for examples.

    No parameters are needed, but the following are allowed:

    ========  =======  ===================================================
    Param     Type     Description
    ========  =======  ===================================================
    version   integer  the Ensembl version number
    species   string   the species identifier (example 'Hs', 'Mm')
    num       integer  Number of ids to return.
    source_db string   Defaults to 'Ensembl', but other are valid, please see
                       external_dbs().
    ========  =======  ===================================================

    If successful, a JSON response will be returned with an array of IDs.

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug(f'Call for: {request.method} {request.url}')

    source_db = request.values.get('source_db', 'Ensembl')
    limit = request.values.get('limit', None)
    release = request.values.get('release', None)
    species = request.values.get('species', None)

    print(f'source_db={source_db}')
    print(f'limit={limit}')
    print(f'release={release}')
    print(f'species={species}')

    ret = {'meta': get.db_meta(release, species),
           'ids': None}

    try:
        ret['ids'] = genes_ensimpl.random_ids(source_db, limit,
                                              release, species)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


