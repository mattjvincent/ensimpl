# -*- coding: utf-8 -*-
import click
from tabulate import tabulate

from ensimpl.utils import configure_logging, get_logger
from ensimpl.fetch import get


@click.command('info', short_help='stats on database')
@click.argument('version', metavar='<version>')
@click.argument('species', metavar='<species>')
@click.option('-v', '--verbose', count=True)
def cli(version, species, verbose):
    """
    Stats annotation database <filename> for <term>
    """
    configure_logging(verbose)
    LOG = get_logger()
    LOG.debug("Stats database...")

    statistics = get.info(version, species)

    print('Version: {}'.format(statistics['version']))
    print('Species: {} {}'.format(statistics['species'], statistics['assembly_patch']))
    arr = []
    for stat in sorted(statistics['stats']):
        arr.append([stat, statistics['stats'][stat]])
    print(tabulate(arr))


