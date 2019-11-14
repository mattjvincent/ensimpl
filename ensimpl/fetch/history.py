# -*- coding: utf_8 -*-

import ensimpl.fetch.genes as genes
import ensimpl.db_config as db_config
import ensimpl.utils as utils

LOG = utils.get_logger()


def get_history(ensembl_id, release_start=None, release_end=None,
                species=None, details=False):
    """Get a genes history.

    Args:
        ensembl_id (str): The Ensembl identifier.
        release_start (str): The start Ensembl release.
        release_end (str): The ense Ensembl release.
        species (str): The Ensembl species identifier.
        details (bool): True to retrieve all information including transcripts,
            exons, proteins.  False will only retrieve the top level gene
            information. Not yet implemented.

    Returns:
        list: A ``list`` of ``dicts`` representing genes.

    Raises:
        Exception: When sqlite error or other error occurs.
    """
    results = {}

    try:
        if release_start is None:
            release_start = min(int(db['release']) for db in db_config.ENSIMPL_DBS)

        if release_end is None:
            release_end = max(int(db['release']) for db in db_config.ENSIMPL_DBS)

        release_start = int(release_start)
        release_end = int(release_end)

        print(release_start, type(release_start), release_end, type(release_end))

        gene_history = {}

        for release in range(release_start, release_end + 1):
            print(release)
            try:
                gene_history[release] = genes.get([ensembl_id], str(release),
                                                  species, details)
            except ValueError as ve:
                LOG.debug(ve)

        results = gene_history

    except ValueError as e:
        LOG.debug(e)

    return results






