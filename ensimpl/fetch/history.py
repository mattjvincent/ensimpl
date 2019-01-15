# -*- coding: utf_8 -*-

import ensimpl.fetch.genes as genes
import ensimpl.db_config as db_config
import ensimpl.utils as utils

LOG = utils.get_logger()


def get_history(ensembl_id, species, version_start=None, version_end=None, full=False):
    """Get a genes history.

    Args:
        version (int): The Ensembl version number.
        species (str): The Ensembl species identifier.
        ids (list): A ``list`` of ``str`` which are Ensembl identifiers.
        full (bool): True to retrieve all information including transcripts,
            exons, proteins.  False will only retrieve the top level gene
            information.

    Returns:
        list: A ``list`` of ``dicts`` representing genes.

    Raises:
        Exception: When sqlite error or other error occurs.
    """
    results = {}

    try:
        if version_start is None:
            version_start = min(db['version'] for db in db_config.ENSIMPL_DBS)

        if version_end is None:
            version_end = max(db['version'] for db in db_config.ENSIMPL_DBS)

        gene_history = {}

        for ver in range(version_start, version_end + 1):
            try:
                gene_history[ver] = genes.get([ensembl_id], species, ver)
            except ValueError as ve:
                LOG.debug(ve)

        results = gene_history

    except ValueError as e:
        LOG.debug(e)

    return results






