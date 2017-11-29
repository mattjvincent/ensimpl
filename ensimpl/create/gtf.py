# -*- coding: utf-8 -*-

from collections import namedtuple, OrderedDict
import time

from ensimpl import utils

gtfInfoFields = ['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'frame', 'attributes']
GTFRecord = namedtuple('GTFRecord', gtfInfoFields)

LOG = utils.get_logger()


class GTFRecordFile(object):
    """
    Simple GTF object for parsing GTF files

    http://blog.nextgenetics.net/?e=27

    Supports transparent gzip decompression.
    """
    def __init__(self, file_name):
        if not file_name:
            raise ValueError("A filename must be supplied")

        self.file_name = file_name
        self.current_line = None
        self.current_record = None
        self.reader = iter(utils.open_resource(file_name))

    def __iter__(self):
        return self

    def __next__(self):
        self.current_line = self.reader.__next__().decode()
        while self.current_line[0] == '#' or self.current_line[0] == '!':
            self.current_line = self.reader.__next__().decode()
        self.current_record = parse_gtf_line(self.current_line)
        return self.current_record


class GTFObject(object):
    def __init__(self, record=None):
        self.seqid = record.seqid if record else None
        self.source = record.source if record else None
        self.type = record.type if record else None
        self.start = record.start if record else None
        self.end = record.end if record else None
        self.strand = record.strand if record else None
        self.score = record.score if record else None
        self.frame = record.frame if record else None
        self.attributes = record.attributes if record else None


class GTFGene(GTFObject):
    def __init__(self, record=None):
        GTFObject.__init__(self, record)
        self.id = None
        self.version = None
        self.name = None

        if record:
            self.id = self.attributes.get('gene_id', None)
            self.version = self.attributes.get('gene_version', None)
            self.name = self.attributes.get('gene_name', None)

        self.transcripts = OrderedDict()

    def add_record(self, record):

        if record.type == 'gene':
            raise ValueError('Cannot add a gene record to this gene: {} [{}]'.format(self.id, record))
        elif record.type == 'transcript':
            transcript_id = record.attributes['transcript_id']

            if transcript_id in self.transcripts:
                raise ValueError('Cannot have more than 1 transcript with the same id: {}'.format(transcript_id))

            self.transcripts[transcript_id] = GTFTranscript(record)
        elif record.type in ('exon', 'start_codon', 'stop_codon', 'CDS'):
            transcript_id = record.attributes['transcript_id']

            if transcript_id not in self.transcripts:
                raise ValueError('Cannot find transcript with the id: {}'.format(transcript_id))

            transcript = self.transcripts[transcript_id]
            transcript.add_record(record)
            self.transcripts[transcript_id] = transcript

    def __str__(self):
        return 'Gene: {}'.format(self.id)


class GTFTranscript(GTFObject):
    def __init__(self, record):
        GTFObject.__init__(self, record)
        self.id = None
        self.version = None
        self.name = None

        if record:
            self.id = self.attributes.get('transcript_id', None)
            self.version = self.attributes.get('transcript_version', None)
            self.name = self.attributes.get('transcript_name', None)

        self.exons = OrderedDict()
        self.protein = None
        self.protein_start = None
        self.protein_end = None

    def add_record(self, record):
        if record.type == 'gene':
            raise ValueError('Cannot add a gene record to a transcript')
        elif record.type == 'transcript':
            raise ValueError('Cannot add a transcript record to another transcript')
        elif record.type in ('exon', 'start_codon', 'stop_codon', 'CDS'):
            transcript_id = record.attributes.get('transcript_id', None)

            if transcript_id != self.id:
                raise ValueError('Incorrect transcript id in record: '.format(transcript_id))

            if record.type == 'exon':
                exon_id = record.attributes.get('exon_id', None)

                if exon_id in self.exons:
                    raise ValueError('Exon id already found: '.format(exon_id))

                self.exons[exon_id] = GTFExon(record)
            elif record.type == 'start_codon':
                self.protein_start = record.start
            elif record.type == 'stop_codon':
                self.protein_end = record.end
            elif record.type == 'CDS':
                if not self.protein:
                    self.protein = GTFProtein(record)


class GTFExon(GTFObject):
    def __init__(self, record):
        GTFObject.__init__(self, record)
        self.id = None
        self.version = None
        self.number = None

        if record:
            self.id = self.attributes.get('exon_id', None)
            self.version = self.attributes.get('exon_version', None)
            self.number = self.attributes.get('exon_number', None)


class GTFProtein(GTFObject):
    def __init__(self, record):
        GTFObject.__init__(self, record)
        self.id = None
        self.version = None

        if record:
            self.id = self.attributes.get('protein_id', None)
            self.version = self.attributes.get('protein_version', None)


class GTFGeneFile(object):
    """
    Simple GTF object for parsing GTF files

    http://blog.nextgenetics.net/?e=27

    Supports transparent gzip decompression.
    """
    def __init__(self, file_name):
        if not file_name:
            raise ValueError("A filename must be supplied")

        self.file_name = file_name
        self.gtf_reader = GTFRecordFile(file_name)
        self.prev_record = None
        self.current_record = None
        self.prev_gene = None
        self.current_gene = None
        self.EOF = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.EOF:
            raise StopIteration()

        if self.prev_record:
            self.current_gene = GTFGene(self.prev_record)
        else:
            self.current_record = self.gtf_reader.__next__()
            self.current_gene = GTFGene(self.current_record)

        self.current_record = self.gtf_reader.__next__()

        try:
            while self.current_record.attributes['gene_id'] == self.current_gene.id:
                self.current_gene.add_record(self.current_record)
                self.current_record = self.gtf_reader.__next__()
        except StopIteration as si:
            self.EOF = True

        self.prev_record = self.current_record
        return self.current_gene


def attributes_to_odict(attributes):
    """
    Parse the GTF attribute column and return a dict
    """
    if attributes == ".":
        return OrderedDict()

    ret = OrderedDict()
    for attribute in attributes.strip().split(";"):
        if len(attribute):
            elems = attribute.strip().split(' ')
            key = elems[0]
            val = ' '.join(elems[1:])
            if val[0] == '"':
                val = val[1:]
            if val[-1] == '"':
                val = val[0:-1]
            ret[key] = val

    return ret


def odict_to_attributes(attributes):
    """
    Parse the GTF attribute column and return a dict
    """
    if attributes:
        atts = []
        for k, v in attributes.items():
            atts.append('{} "{}"'.format(k, v))
        temp_atts = "; ".join(atts)
        return temp_atts.rstrip() + ";"

    return '.'


def parse_gtf_line(line):
    """
    Parse the GTF line.

    :param line: a line from GTF file
    :type line: str
    :return:
    """
    elem = line.strip().split("\t")

    # If this fails, the file format is not standard-compatible
    if len(elem) != len(gtfInfoFields):
        LOG.error(line)
        LOG.error("{0} != {1}".format(len(elem), len(gtfInfoFields)))
        raise IOError("Improperly formatted GTF file")

    data = {
        'seqid': None if elem[0] == '.' else elem[0].replace('"', ''),
        'source': None if elem[1] == '.' else elem[1].replace('"', ''),
        'type': None if elem[2] == '.' else elem[2].replace('"', ''),
        'start': None if elem[3] == '.' else int(elem[3]),
        'end': None if elem[4] == '.' else int(elem[4]),
        'score': None if elem[5] == '.' else float(elem[5]),
        'strand': None if elem[6] == '.' else elem[6].replace('"', ''),
        'frame': None if elem[7] == '.' else elem[7].replace('"', ''),
        'attributes': attributes_to_odict(elem[8])
    }

    return GTFRecord(**data)


def gtf_to_genes(file_name):
    """Parse the GTF file by genes.

    Args:
        file_name (str): the GTF file name

    Returns:
        dict: dictionary with keys being ensembl_ids and values being
            ``ensimpl.create.GTFGene`` objects
    """
    start = time.time()

    genes = {}
    counter = 0
    gtf_file = GTFGeneFile(file_name)

    for gene in gtf_file:
        if counter and counter % 1000 == 0:
            LOG.info("Parsed {0:,} genes".format(counter))

        genes[gene.id] = gene
        counter += 1

    LOG.info('GTF File parsed: {0}'.format(utils.format_time(start, time.time())))

    return genes
