# -*- coding: utf-8 -*-
import sys
import time

import click
import json
from tabulate import tabulate

from ensimpl.utils import configure_logging, format_time, get_logger
from ensimpl.fetch import search as search_ensimpl


@click.command('search', short_help='search for data')
@click.argument('term', metavar='<term>')
@click.option('-e', '--exact', is_flag=True)
@click.option('-f', '--format', 'display', default='pretty',
              type=click.Choice(['tab', 'csv', 'json', 'pretty']))
@click.option('-m', '--max', default=-1)
@click.option('-s', '--species', type=click.Choice(['mm', 'hs']))
@click.option('--ver', default=None)
@click.option('-v', '--verbose', count=True)
def cli(term, ver, exact, display, max, species, verbose):
    """
    Search ensimpl database <filename> for <term>
    """
    configure_logging(verbose)
    LOG = get_logger()
    LOG.info("Search database...")

    maximum = max if max >= 0 else None

    try:
        tstart = time.time()
        result = search_ensimpl.search(term, ver, species, exact, maximum)
        tend = time.time()

        LOG.debug("Num Results: {}".format(result.num_results))
        count = 0

        if len(result.matches) == 0:
            print("No results found")
            sys.exit()

        headers = ["ID", "SYMBOL", "IDS", "POSITION", "MATCH_REASON",
                   "MATCH_VALUE"]
        tbl = []

        if display in ('tab', 'csv'):
            delim = '\t' if display == 'tab' else ','
            print(delim.join(headers))

        for match in result.matches:
            line = list()
            line.append(match.ensembl_gene_id)
            line.append(match.symbol)

            if match.external_ids:
                ext_ids = []
                for ids in match.external_ids:
                    ext_ids.append('{}/{}'.format(ids['db'], ids['db_id']))
                line.append('||'.join(ext_ids))
            else:
                line.append('')

            line.append("{}:{}-{}".format(match.chromosome,
                                          match.position_start,
                                          match.position_end))
            line.append(match.match_reason)
            line.append(match.match_value)

            if display in ('tab', 'csv'):
                print(delim.join(map(str, line)))
            elif display == 'json':
                tbl.append(dict(zip(headers, line)))
            else:
                tbl.append(line)

            count += 1
            if count >= max > 0:
                break

        if display in ('tab', 'csv'):
            pass
        elif display == 'json':
            print(json.dumps({'data': tbl}, indent=4))
        else:
            print(tabulate(tbl, headers))

        LOG.info("Search time: {}".format(format_time(tstart, tend)))

    except Exception as e:
        LOG.error('Error: {}'.format(e))

