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
from ensimpl.fetch import search as search_ensimpl

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
        the Javascript response object
    """
    headers = {'Content-Type': 'application/javascript'}
    return render_template('ensimpl.js'), 200, headers


@api.route("/versions")
@support_jsonp
def versions():
    """Get the Ensimpl Javascript file.

    Returns:
        JSON
    """
    version_info = []

    for it in db_config.ENSIMPL_DBS:
        meta = get.meta(it['version'], it['species'])
        version_info.append(meta)

    return jsonify(version_info)


@api.route("/chromosomes")
@support_jsonp
def chromosomes():
    """Get the chromosome information.

    Returns:
        JSON
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    species = request.values.get('species', None)

    ret = {'version': None, 'species': None, 'chromosomes': None}

    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        meta = get.meta(version)
        ret['version'] = meta['version']
        ret['species'] = meta['species'][species]

        all_chromosomes = get.chromosomes(version, species)
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

    Returns:
        JSON
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    species = request.values.get('species', None)

    ret = {'version': None, 'chromosomes': {}}

    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        meta = get.meta(version, species)
        ret['version'] = meta['version']
        ret['species'] = meta['species']

        all_karyotypes = get.karyotypes(version, species)
        ret['chromosomes'] = all_karyotypes

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/gene")
@support_jsonp
def gene():
    """Get the gene information.

    Returns:
        JSON
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    species = request.values.get('species', None)
    id = request.values.get('id', None)

    ret = {'gene': None}

    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        if not id:
            raise ValueError('No id specified')

        results = genes_ensimpl.get(ids=[id],
                                    full=True,
                                    version=version,
                                    species_id=species)

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


@api.route("/genes")
@support_jsonp
def genes():
    """Get the gene(s) information.

    Returns:
        JSON
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    ids = request.values.getlist('id', None)
    full = ensimpl_utils.str2bool(request.values.get('full', 'F'))
    species = request.values.get('species', None)

    ret = {'genes': None}

    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        if not ids:
            raise ValueError('No ids specified')

        results = genes_ensimpl.get(ids=ids,
                                    full=full,
                                    version=version,
                                    species_id=species)

        if len(results) == 0:
            current_app.logger.info("No results found")
            return jsonify(ret)

        ret['genes'] = results

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/search")
@support_jsonp
def search():
    """Perform a search

    Returns:
        JSON
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    term = request.values.get('term', None)
    species = request.values.get('species', None)
    exact = ensimpl_utils.str2bool(request.values.get('exact', 'F'))
    limit = request.values.get('limit', '1000000')

    try:
        limit = int(limit)
    except ValueError as ve:
        limit = 1000000
        current_app.logger.info(ve)

    request_params = {'term': term, 'species': species, 'exact': exact,
                      'limit': limit, 'version': version}

    ret = {'request': request_params, 'result': {'num_results': 0,
                                                 'num_matches': 0,
                                                 'matches': None}}
    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        results = search_ensimpl.search(term=term,
                                        version=version,
                                        species_id=species,
                                        exact=exact,
                                        limit=limit)

        if len(results.matches) == 0:
            current_app.logger.info("No results found")
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



