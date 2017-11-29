# -*- coding: utf-8 -*-

import json
import time

from urllib.parse import urlparse, urlencode
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from ensimpl import utils

LOG = utils.get_logger()

ENSEMBL_REST_SERVER = 'https://rest.ensembl.org'
ENSEMBL_REST_ENDPOINT = '/xrefs/id/'

DATABASES = {'HGNC': ['primary_id', 'synonyms', 'description'],
             'EntrezGene': ['primary_id', 'synonyms'],
             'Uniprot_gn': ['primary_id'],
             'MGI': ['primary_id', 'synonyms', 'description'],
             'Clone_based_ensembl_gene': ['primary_id']
             }


class EnsemblRestClient(object):
    """Modified version from
    https://github.com/Ensembl/ensembl-rest/wiki/Example-Python-Client
    """
    def __init__(self, server='http://rest.ensembl.org', reqs_per_sec=15):
        self.server = server
        self.reqs_per_sec = reqs_per_sec
        self.req_count = 0
        self.last_req = 0

    def perform_rest_action(self, endpoint, hdrs=None, params=None, retries=5):
        if hdrs is None:
            hdrs = {}

        if 'Content-Type' not in hdrs:
            hdrs['Content-Type'] = 'application/json'

        if params:
            endpoint += '?' + urlencode(params)

        data = None

        # check if we need to rate limit ourselves
        if self.req_count >= self.reqs_per_sec:
            delta = time.time() - self.last_req
            if delta < 1:
                time.sleep(1 - delta)
            self.last_req = time.time()
            self.req_count = 0

        try:
            request = Request(self.server + endpoint, headers=hdrs)
            response = urlopen(request)
            content = response.read()
            if content:
                data = json.loads(content.decode())
            self.req_count += 1

        except HTTPError as e:
            print('retries={}'.format(retries))

            # check if we are being rate limited by the server
            if retries == 0:
                return None

            if e.code == 429:
                LOG.warn('Ensembl is RATE limiting out request')
                LOG.warn(str(e.headers))

                if 'Retry-After' in e.headers:
                    retry = e.headers['Retry-After']
                    time_sleep = float(retry) + 1  # add a second just in case
                    LOG.warn('Sleeping: {}'.format(time_sleep))
                    time.sleep(time_sleep)
                    return self.perform_rest_action(endpoint, hdrs, params, retries-1)
            else:
                LOG.error('Request failed for {0}: Status code: {1.code} Reason: {1.reason}\n'.format(endpoint, e))

        return data

    def get_gene_information(self, gene_id):
        dbs = list(DATABASES.keys())

        json_data = self.perform_rest_action('/xrefs/id/{0}'.format(gene_id),
                                             params=None)

        ids = []
        synonyms = []
        description = None

        for data in json_data:
            if data['dbname'] in dbs:
                for d in DATABASES[data['dbname']]:
                    if d == 'primary_id':
                        ids.append({'db_name': data['dbname'],
                                    'id': data[d]})
                    elif d == 'synonyms':
                        synonyms.extend(data[d])
                    elif d == 'description':
                        description = data[d]

        return {'ids': ids, 'synonyms': synonyms, 'description': description}


