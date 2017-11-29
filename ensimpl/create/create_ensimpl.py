# -*- coding: utf-8 -*-

from collections import namedtuple
import io
import os
import time

import ensimpl.utils as utils
import ensimpl.create.ensembl_db as ensembl_db
import ensimpl.create.ensimpl_db as ensimpl_db

DEFAULT_CONFIG = 'ftp://ftp.jax.org/churchill-lab/ensimpl/ensimpl.ensembl.conf'

ENSEMBL_FIELDS = ['version', 'release_date', 'assembly', 'assembly_patch',
                  'species_id', 'species_name', 'db', 'server', 'port',
                  'user_id', 'password']
EnsemblReference = namedtuple('EnsemblReference', ENSEMBL_FIELDS)


EXTERNAL_IDS = {'HGNC': 'HGNC',
                'EntrezGene': 'EntrezGene',
                'Uniprot_gn': 'Uniprot',
                'MGI': 'MGI'
                }


LOG = utils.get_logger()


def parse_config(resource_name):
    """Take a resource string (file name, url) and open it.  Parse the file.

    Args:
        resource_name (str): the name of the resource

    Returns:
        dict: :obj:`EnsemblReference` with the
            key being the Ensembl version
    """
    start = time.time()
    all_releases = {}
    line = ''

    try:
        with utils.open_resource(resource_name) as fd:
            buffer = io.BufferedReader(fd)
            buffer.readline()  # skip first line

            # parse each line and create an EnsemblReference
            for line in buffer:
                line = str(line, 'utf-8')
                elems = line.strip().split('\t')
                if len(elems) == 10:
                    elems.append(None)
                reference = EnsemblReference(*elems)
                release = all_releases.get(reference.version, {})
                release[reference.species_id] = reference
                all_releases[reference.version] = release
    except IOError as io_error:
        LOG.error('Unable to access resource: {}'.format(resource_name))
        LOG.debug(io_error)
        all_releases = {}
    except TypeError as type_error:
        LOG.error('Unable to parse resource: {}'.format(resource_name))
        LOG.debug(type_error)
        LOG.debug('Error on the following:')
        LOG.debug(line)

        all_releases = {}

    LOG.info('Config parsed in {}'.format(
        utils.format_time(start, time.time())))

    return all_releases


def create(ensembl, directory, resource):
    """Create ensimpl database(s).

    Args:
        ensembl (list): all Ensembl versions to create, None for all
        directory: directory to use
        resource: configuration to parse for Ensembl information
    """
    if ensembl:
        LOG.debug('Ensembl Versions: {}'.format(','.join(ensembl)))
    else:
        LOG.debug('Ensembl Versions: ALL')

    LOG.debug('Directory: {}'.format(directory))
    LOG.debug('Resource: {}'.format(resource))

    releases = parse_config(resource)

    if not releases:
        LOG.error('Unable to determine the Ensembl releases and locations '
                  'for download')
        LOG.error('Please make sure that the resource "{}" is accessible '
                  'and in the correct format'.format(resource))
        raise Exception("Unable to create databases")

    if ensembl:
        all_releases = list(releases.keys())
        all_releases.sort()
        not_found = list(set(ensembl) - set(all_releases))
        if not_found:
            not_found.sort()
            LOG.error('Unable to determine the Ensembl release '
                      'specified: {}'.format(', '.join(not_found)))
            LOG.error('Please make sure that the resource "{}" is '
                      'accessible and in the correct format'.format(resource))
            LOG.error('Found Ensembl versions:'
                      ' {}'.format(', '.join(all_releases)))
            raise Exception("Unable to create databases")

    for release_version, release_value in sorted(releases.items()):
        if ensembl and release_version not in ensembl:
            continue

        LOG.warn('Generating ensimpl database for '
                 'Ensembl version: {}'.format(release_version))

        ensimpl_file = 'ensimpl.{}.db3'.format(release_version)
        ensimpl_file = os.path.join(directory, ensimpl_file)
        utils.delete_file(ensimpl_file)

        LOG.info('Creating: {}'.format(ensimpl_file))
        ensimpl_db.initialize(ensimpl_file)

        for species_id, ensembl_reference in sorted(release_value.items()):
            LOG.info('Extracting chromosomes...')
            chromosomes = ensembl_db.extract_chromosomes(ensembl_reference)

            LOG.info('Extracting genes...')
            genes = ensembl_db.extract_ensembl_genes(ensembl_reference)

            LOG.info('Extracting synonyms...')
            synonyms = ensembl_db.extract_synonyms(ensembl_reference)

            LOG.info('Extracting transcript, protein, and exon information...')
            gtep = ensembl_db.extract_ensembl_gtpe(ensembl_reference)

            LOG.info('Inserting chromsomes..')
            ensimpl_db.insert_chromosomes(ensimpl_file, ensembl_reference,
                                          chromosomes)

            LOG.info('Inserting genes..')
            ensimpl_db.insert_genes(ensimpl_file, ensembl_reference, genes,
                                    synonyms)

            LOG.info('Inserting transcript, protein, and exon information...')
            ensimpl_db.insert_gtpe(ensimpl_file, ensembl_reference, gtep)

        LOG.info('Finalizing...')
        ensimpl_db.finalize(ensimpl_file, release_value)
        LOG.info('DONE')


