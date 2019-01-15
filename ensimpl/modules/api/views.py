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


@api.route("/js/ensimpl.js")
def ensimpl_js():
    """Get the Ensimpl Javascript file.

    Returns:
        :class:`flask.Response`: The response which is the Javascript file.
    """
    headers = {'Content-Type': 'application/javascript'}
    return render_template('api/ensimpl.js'), 200, headers


@api.route("/versions")
@support_jsonp
def versions():
    """Get the version and species information.

    No parameters are needed.

    If successful, a JSON response will be returned with a single
    ``version`` element containing a ``list`` of versions consisting of the
    following items:

    ==============  =======  ==================================================
    Param           Type     Description
    ==============  =======  ==================================================
    version         integer  the Ensembl version number
    species         string   the species identifier (example 'Hs', 'Mm')
    assembly        string   the genome assembly information
    assembly_patch  string   the genome assembly patch number
    ==============  =======  ==================================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    version_info = []

    try:
        for it in db_config.ENSIMPL_DBS:
            meta = get.meta(it['species'], it['version'])
            version_info.append(meta)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify({'versions': version_info})


@api.route("/info")
@support_jsonp
def info():
    """Get the information for a particular Ensembl version and species.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``version`` element containing a ``list`` of versions consisting of the
    following items:

    ==============  =======  ==================================================
    Param           Type     Description
    ==============  =======  ==================================================
    version         integer  the Ensembl version number
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
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    species = request.values.get('species', None)
    version = request.values.get('version', None)

    ret = {}

    try:
        info = get.info(species, version)
        ret['info'] = info

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/chromosomes")
@support_jsonp
def chromosomes():
    """Get the chromosome information.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``chromosomes`` element containing a ``list`` of chromosomes consisting the
    following items:

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
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    species = request.values.get('species', None)
    version = request.values.get('version', None)

    ret = {'version': None, 'species': None, 'chromosomes': None}

    try:
        meta = get.meta(species, version)
        ret['version'] = meta['version']
        ret['species'] = meta['species']
        ret['assembly'] = meta['assembly']
        ret['assembly_patch'] = meta['assembly_patch']

        all_chromosomes = get.chromosomes(species, version)

        ret['chromosomes'] = all_chromosomes

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/karyotypes")
@support_jsonp
def karyotypes():
    """Get the karyotype information.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    =======  =======  ===================================================

    If successful, a JSON response will be returned with a single
    ``chromosomes`` element containing a ``list`` of chromosomes consisting the
    following items:

    =================  =======  ============================================
    Element            Type     Description
    =================  =======  ============================================
    chromosome         string   chromosome
    order              integer  chromosome order in the genome
    length             int      length of chromosome in base pairs
    karyotypes         list     each element being a ``karyotype_element``
    =================  =======  ============================================

    A ``karyotype_element`` contains the follwoing items:

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
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    species = request.values.get('species', None)
    version = request.values.get('version', None)

    ret = {'version': None, 'chromosomes': {}}

    try:
        meta = get.meta(species, version)
        ret['version'] = meta['version']
        ret['species'] = meta['species']
        ret['assembly'] = meta['assembly']
        ret['assembly_patch'] = meta['assembly_patch']

        all_karyotypes = get.karyotypes(species, version)
        ret['chromosomes'] = all_karyotypes

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/gene")
@support_jsonp
def gene():
    """Get the information for an Ensembl gene.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    id       string   the Ensembl id
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
    external_ids       list     each having keys of 'db' and 'db_id
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
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    species = request.values.get('species', None)
    version = request.values.get('version', None)
    id = request.values.get('id', None)
    full = ensimpl_utils.str2bool(request.values.get('full', 0))

    ret = {'gene': None}

    try:
        if not id:
            raise ValueError('No id specified')

        results = genes_ensimpl.get(ids=[id],
                                    full=full,
                                    version=version,
                                    species=species)

        if len(results) == 0:
            current_app.logger.info("No results found")
            return jsonify(ret)

        if len(results) > 1:
            msg = 'Too many genes found for: {}'.format(id)
            raise ValueError(msg)

        ret['gene'] = results

    except Exception as e:
        current_app.logger.error(str(e))
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/genes", methods=['POST'])
@support_jsonp
def genes():
    """Get the information for an Ensembl gene.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    id       list     repeated id elements, one per Ensembl id
    full     string   True for all information, False for high level
    =======  =======  ===================================================

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
    external_ids       list     each having keys of 'db' and 'db_id
    =================  =======  ============================================

    If ``full`` is ``True``, each gene will also contain the following:

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

    If an id is not found, the gene will still be returned but have
    ``null`` for a value.

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug('Call for: POST {}'.format(request.url))

    version = None
    species = None
    ids = None
    full = False

    if request.is_json:
        if 'version' in request.json:
            version = request.json['version']
        if 'species' in request.json:
            species = request.json['species']
        if 'ids[]' in request.json:
            ids = request.json['ids[]']
        if 'full' in request.json:
            full = ensimpl_utils.str2bool(request.json['full'])
    else:
        version = request.values.get('version', None)
        ids = request.values.getlist('ids[]', None)
        full = ensimpl_utils.str2bool(request.values.get('full', 'F'))
        species = request.values.get('species', None)

    ret = {'ids': None}

    try:
        if len(ids) == 0:
            raise ValueError('No ids specified')
        elif len(ids) == 1 and len(ids[0]) == 0:
            raise ValueError('No ids specified')

        results = genes_ensimpl.get(version=version,
                                    species=species,
                                    ids=ids,
                                    full=full)

        if len(results) == 0:
            current_app.logger.info("No results found")
            return jsonify(ret)

        ret['ids'] = results

    except Exception as e:
        print(str(e))
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/search")
@support_jsonp
def search():
    """Perform a search of a Ensimpl database.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    term     string   the term to search for
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
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    species = request.values.get('species', None)
    term = request.values.get('term', None)
    exact = ensimpl_utils.str2bool(request.values.get('exact', 'F'))
    limit = request.values.get('limit', '100000')

    try:
        limit = int(limit)
    except ValueError as ve:
        limit = 100000
        current_app.logger.info(ve)

    request_params = {'term': term, 'species': species, 'exact': exact,
                      'limit': limit, 'version': version}

    current_app.logger.debug('PARAMS: {}'.format(request_params))

    ret = {'request': request_params, 'result': {'num_results': 0,
                                                 'num_matches': 0,
                                                 'matches': None}}

    try:
        results = search_ensimpl.search(term=term,
                                        version=version,
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


@api.route("/history")
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
    current_app.logger.debug('Call for: {}'.format(request.url))

    ensembl_id = request.values.get('id', None)
    species = request.values.get('species', None)

    version_start = fetch_utils.nvli(request.values.get('version_start', None), None)
    version_end = fetch_utils.nvli(request.values.get('version_end', None), None)

    request_params = {'ensembl_id': ensembl_id, 'species': species,
                      'version_start': version_start, 'version_end': version_end}

    current_app.logger.debug('PARAMS: {}'.format(request_params))

    ret = {'request': request_params, 'history': None}
    try:
        if not ensembl_id:
            raise ValueError('No ensembl id specified')

        if not species:
            raise ValueError('No species specified')

        results = genes_history.get_history(ensembl_id, species, version_start, version_end)

        if len(results) == 0:
            current_app.logger.info('No results found')
            return jsonify(ret)

        ret['history'] = results

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)












