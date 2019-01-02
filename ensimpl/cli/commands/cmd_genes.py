# -*- coding: utf-8 -*-
import click
import json
import time

from tabulate import tabulate

from ensimpl.utils import configure_logging, format_time, get_logger
from ensimpl.fetch import genes as get_genes


@click.command('genes', short_help='get all genes')
@click.option('-f', '--format', 'display', default='pretty',
              type=click.Choice(['tab', 'csv', 'json', 'pretty']))
@click.option('-i', '--ids', metavar='<ensembl_ids>',
              type=click.Path(exists=True, resolve_path=True,
                              dir_okay=False, writable=False))
@click.option('-s', '--species', type=click.Choice(['Mm', 'Hs']))
@click.option('--ver', default=None)
@click.option('-v', '--verbose', count=True)
def cli(display, ids, species, ver, verbose):
    """
    Get gene information from annotation database.
    """
    configure_logging(verbose)
    LOG = get_logger()
    LOG.debug("Ensimpl Version: {}".format(ver))
    LOG.debug("Species: {}".format(species))
    LOG.debug("Format: {}".format(display))
    LOG.debug("Ids: {}".format(ids))

    ensembl_ids = None

    if ids:
        ensembl_ids = []
        with open(ids) as fd:
            for row in fd:
                row = row.strip()
                if row:
                    ensembl_ids.append(row.strip().split()[0])

    tstart = time.time()

    result = get_genes.get(ver, species, ensembl_ids, True)
    tend = time.time()

    headers = ['ID', 'VERSION', 'SPECIES', 'SYMBOL', 'NAME', 'SYNONYMS',
               'EXTERNAL_IDS', 'CHR', 'START', 'END', 'STRAND']

    tbl = []

    delim = '"\t"' if display == 'tab' else '","'

    if display in ('tab', 'csv'):
        print('"{}"'.format(delim.join(headers)))

    for i in result:
        r = result[i]
        line = list()
        line.append(r['id'])
        line.append(r.get('ensembl_version', ''))
        line.append(r['species_id'])
        line.append(r.get('symbol', ''))
        line.append(r.get('name', ''))
        line.append('||'.join(r.get('synonyms', [])))

        external_ids = r.get('external_ids', [])
        external_ids_str = ''
        if external_ids:
            ext_ids_tmp = []
            for ext in external_ids:
                ext_ids_tmp.append('{}/{}'.format(ext['db'], ext['db_id']))
            external_ids_str = '||'.join(ext_ids_tmp)
        line.append(external_ids_str)

        line.append(r['chromosome'])
        line.append(r['start'])
        line.append(r['end'])
        line.append(r['strand'])

        if display in ('tab', 'csv'):
            print('"{}"'.format(delim.join(map(str, line))))
        elif display == 'json':
            tbl.append(r)
        else:
            tbl.append(line)

    if display in ('tab', 'csv'):
        pass
    elif display == 'json':
        print(json.dumps({'data': tbl}, indent=2))
    else:
        print(tabulate(tbl, headers))
    pass

    LOG.info("Search time: {}".format(format_time(tstart, tend)))


