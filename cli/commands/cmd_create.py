# -*- coding: utf-8 -*-
import time

import click

from ensimpl.utils import configure_logging, format_time, get_logger
import ensimpl.create.create_ensimpl as create_ensimpl


@click.command('create', options_metavar='<options>',
             short_help='create an annotation database')
@click.option('-d', '--directory', default='.',
              type=click.Path(file_okay=False, exists=True,
                              resolve_path=True, dir_okay=True))
@click.option('-r', '--resource', default=create_ensimpl.DEFAULT_CONFIG)
@click.option('--ver', multiple=True)
@click.option('-v', '--verbose', count=True)
def cli(directory, resource, ver, verbose):
    """
    Creates a new ensimpl database <filename> using Ensembl <version>.
    """
    configure_logging(verbose)
    LOG = get_logger()
    LOG.info("Creating database...")

    if ver:
        ensembl_versions = list(ver)
    else:
        ensembl_versions = None

    tstart = time.time()
    create_ensimpl.create(ensembl_versions, directory, resource)
    tend = time.time()

    LOG.info("Creation time: {}".format(format_time(tstart, tend)))


