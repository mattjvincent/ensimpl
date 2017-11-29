# -*- coding: utf-8 -*-
import click
from tabulate import tabulate

from ensimpl.utils import configure_logging, get_logger
from ensimpl.fetch import get


@click.command('info', short_help='stats on database')
@click.option('--ver', default=None)
@click.option('-v', '--verbose', count=True)
def cli(ver, verbose):
    """
    Stats annotation database <filename> for <term>
    """
    configure_logging(verbose)
    LOG = get_logger()
    LOG.debug("Stats database...")

    statistics = get.info(ver)

    print('Version: {}'.format(statistics['version']))
    for (k, v) in sorted(statistics['species'].items()):
        print('Species: {} {}'.format(k, v['assembly']))
        print(tabulate(v['stats']))


