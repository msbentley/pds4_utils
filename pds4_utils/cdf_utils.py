#!/usr/bin/python
"""
cdf_utils.py - a collection of utilities for dealing with CDF files

Eventually the code in this module should be integrated along with fits_utils,
tiff_utils and pd_to_pds4 into an integrate package to create PDS4 products from
different inputs.

"""

import logging, struct, os, collections
log = logging.getLogger(__name__)

cdf3_magic = b'\xcd\xf3\x00\x01\x00\x00\xff\xff'

record_types = {
    1: 'CDR',       # CDF Descriptor Record
    2: 'GDR',       # Global Descriptor Record
    3: 'rVDR',      # rVariable Descriptor Record
    4: 'ADR',       # Attribute Descriptor Record
    5: 'AgrEDR',    # Attribute g/rEntry Descriptor Record
    6: 'VXR',       # Variable Index Record
    7: 'VVR',       # Variable Values Record
    8: 'zVDR',      # Variable Descriptor Record
    9: 'AzEDR',     # Attribute zEntry Descriptor Record
    10: 'CCR',      # Compressed CDF Record
    11: 'CPR',      # Compression Parameters Record
    12: 'SPR',      # Sparseness Parameters Record
    13: 'CVVR',     # Compressed Variable Values Record
    -1: 'UIR' }     # Unused Internal Record

attribute_scopes = {
    1: 'global',
    2: 'variable',
    3: 'assumed global',
    4: 'assumed variable' }

# Mapping of CDF data types to PDS4 - not completed/used yet
data_types = {
    1: 0,  # 1-byte signed integer
    2: 0,  # 2-byte signed integer
    4: 0,  # 4-byte signed integer
    8: 0,  # 8-byte signed integer
    11: 0 , # 1-byte unsigned integer
    12: 0 , # 2-byte unsigned integer
    14: 0 , # 4-byte unsigned integer
    41: 0, # 1-byte signed integer (same as 1)
    21: 0, # 4-byte single precision floating point
    22: 0, # 8-byte double precision floating point
    44: 0, # 4-byte single precision floating point (same as 21)
    45: 0, # 8-byte double precision floating point (same as 22)
    31: 0, # 8-byte double precision floating point (same as 22) - but for number of ms since 1-Jan-0000 00:00:00.000
    32: 0, # 2 x 8-byte double precision floating point (high resolution epoch)
    33: 0, # 8-byte signed integer - CDF_TIME_TT2000
    51: 0} # 1-byte signed character (ASCII)


# Using struct not bitstring here to reduce dependencies - could change this
# in the future?

record_header_fmt = '>qi'
record_header_size = struct.calcsize(record_header_fmt)
record_header_names = collections.namedtuple("record_header_names", "rec_size rec_type")

cdr_fmt = '>qiq9i256s'
cdr_size = struct.calcsize(cdr_fmt)
cdr_names = collections.namedtuple("cdr_names", "RecordSize RecordType GDRoffset Version Release\
    Encoding Flags rfuA rfuB Increment rfuD rfuE Copyright")


gdr_fmt = '>qi4q5iq3i'
gdr_size = struct.calcsize(gdr_fmt)
gdr_names = collections.namedtuple("gdr_names","RecordSize RecordType rVDRhead zVDRhead ADRhead eof\
    NrVars NumAttr rMaxRec rNumDims NzVars UIRhead rfuC rfuD rfuE")

adr_fmt = '>qi2q5iq3i256s'
adr_size = struct.calcsize(adr_fmt)
adr_names = collections.namedtuple("adr_names","RecordSize RecordType ADRnext AgrEDRhead Scope\
    Num NgrEntries MAXgrEntry rfuA AzEDRhead NzEntries MAXzEntry rfuE Name")    

aedr_fmt = '>qiq9i' # description of attribute entry (AgrEDR for g/r entries and AzEDR for zEntry)
aedr_size = struct.calcsize(aedr_fmt)
aedr_names = collections.namedtuple("aedr_names","RecordSize RecordType AEDRnext AttrNum DataType\
    Num NumElems rfuA rfuB rfuC rfuD rfuE")

zvdr_fmt = '>qiq2i2q7iqi256s3i'
zvdr_size = struct.calcsize(zvdr_fmt)
zvdr_names = collections.namedtuple("zvdr_names","RecordSize RecordType VDRnext DataType MaxRec\
    VXRhead VXRtail Flags SRecords rfuB rfuC rfuF NumElems Num CPRorSPRoffset BlockingFactor\
    Name zNumDims zDimSizes DimVarys")

vxr_fmt = '>qiq2i'
vxr_size = struct.calcsize(vxr_fmt)
vxr_names = collections.namedtuple("vxr_names","RecordSize RecordType VXRnext Nentries NusedEntries")


class cdf:
    """This class provides a few methods for working with CDFs. It is
    not yet feature-complete, but should list the types of records,
    check for fragmentation, and output the byte-offsets of arrays"""

    def __init__(self, cdf_file=None, summary=False):

        self.filename = cdf_file
        self.cdf = None
        self.adrs = []
        self.zvdrs = []
        self.array_offsets = []

        if cdf_file is not None:
            if not os.path.isfile(cdf_file):
                log.error('CDF file does not exist!')
                return None
            self.read_bytes(cdf_file)
            self.find_records()
            if summary:
                self.summarise(outp=True)


    def read_bytes(self, cdf_file):

        f = open(cdf_file, 'rb')
        cdf = bytearray(os.path.getsize(cdf_file))
        f.readinto(cdf)

        if cdf[0:8] != cdf3_magic:
            log.error('%s is not a valid CDF file!' % cdf_file)
            return None

        self.cdf = cdf


    def find_records(self):
        """Finds the byte offsets of all records in a CDF file for further analysis"""

        num_recs = 0
        offset = 8
        rec_info = []

        # first read the CDR to determine if e.g. a checksum is present
        # (which allows us to set the length of usable data and avoid
        # treating the checksum as an internal record)
        self.read_cdr()
        self.datalen = len(self.cdf)
        if self.cdr.Flags>>3 or self.cdr.Flags>>4: # checksum present
            self.datalen -= 16


        while(offset < self.datalen):

            rec = {}
            rec_header = record_header_names(*struct.unpack_from(record_header_fmt, self.cdf, offset))
            rec_info.append((offset, rec_header.rec_type))
            log.debug(offset, rec_header.rec_type, record_types[rec_header.rec_type])
            offset = offset + rec_header.rec_size
            num_recs += 1

        self.rec_info = rec_info
        log.info('CDF file read with %d records' % len(rec_info))


    def summarise(self, outp=False):

        rec_count = dict((el, 0) for el in record_types.values())

        for offset, rec_type in self.rec_info:
            rec_count[record_types[rec_type]] += 1

        for rec in rec_count:
            if rec_count[rec] > 0:
                if outp: print('Type: %s, Count: %d' % (rec, rec_count[rec]) )

        return rec_count


    def read_gdr(self):
        offset = [rec[0] for rec in self.rec_info if rec[1]==2 ][0]
        gdr = gdr_names(*struct.unpack_from(gdr_fmt, self.cdf, offset))
        print(gdr)

    def read_cdr(self):
        offset = 8
        self.cdr = cdr_names(*struct.unpack_from(cdr_fmt, self.cdf, offset))


    def read_vxr(self):
        offsets = [rec[0] for rec in self.rec_info if rec[1]==6 ]
        for offset in offsets:
            vxr = vxr_names(*struct.unpack_from(vxr_fmt, self.cdf, offset))
            
            # based on Nentries we have 3 arrays:
            # first (signed 4-byte)
            # last (signed 4-byte)
            # offset (signed 8-byte)
            
            data_offset = offset + vxr_size
            first = struct.unpack(">%ii" % vxr.Nentries, self.cdf[data_offset:data_offset+4*vxr.Nentries])
            data_offset = data_offset + 4*vxr.Nentries
            last = struct.unpack(">%ii" % vxr.Nentries, self.cdf[data_offset:data_offset+4*vxr.Nentries])
            data_offset = data_offset + 4*vxr.Nentries
            foffsets = struct.unpack(">%iq" % vxr.Nentries, self.cdf[data_offset:data_offset+8*vxr.Nentries])
            
            self.array_offsets.append(foffsets[0] + 12) # 12 bytes

        return
        

    def read_adrs(self):
        offsets = [rec[0] for rec in self.rec_info if rec[1]==4 ]
        for offset in offsets:
            adr = adr_names(*struct.unpack_from(adr_fmt, self.cdf, offset))
            self.adrs.append(adr)            
            
            aedr = self.read_aedr(adr.AgrEDRhead) if adr.Scope==1 else self.read_aedr(adr.AzEDRhead)
            # not yet storing/returning this!

        return


    def read_gentry(self, att_num):
        """Read the global entry of the specified attribute"""

        if len(self.adrs)==0:
            self.read_adrs()

        adr = [adr for adr in self.adrs if adr.Num == att_num]
        if adr is None:
            log.error('Attribute number %d not found' % att_num)
        else:
            adr = adr[0]

        for entry in range(1, adr.NgrEntries+1):
            print(entry)

        return adr


    def read_aedr(self, offset):
        """Read the attribute definition record"""
        
        aedr = aedr_names(*struct.unpack_from(aedr_fmt, self.cdf, offset))
        print(aedr)
        
        # aedr.DataType = numeric description of attribute type
        # aedr.NumElems = number of elements of DataType
        # data is read from (offset + aedr_size) to (offset + aedr_size) + aedr.NumElements * data_size
        
        data_size = 0 # TODO
        
        data = self.cdf[ (offset+aedr_size):(offset+aedr_size+aedr.NumElems) ]


    def read_zvdrs(self):

        offsets = [rec[0] for rec in self.rec_info if rec[1]==8 ]
        for offset in offsets:
            zvdr = zvdr_names(*struct.unpack_from(zvdr_fmt, self.cdf, offset))
            self.zvdrs.append(zvdr)
            # log.info('Processed variable: %s' % (strip_null(zvdr.Name)))


    def is_fragmented(self):
        """Performs a simple fragmentation check"""

        # for a file with no framentation num zVDRs = num VXR = num VVR
        rec_count = self.summarise(outp=False)
        if (rec_count['zVDR'] != rec_count['VVR']) or (rec_count['zVDR'] != rec_count['VXR']):
            return True
        else:
            return False


    def get_offsets(self, show=False):
        """Gets (and optionally prints) the byte offsets to each array in the CDF file, in order to produce
        the PDS4 label correctly"""

        self.read_zvdrs()
        self.read_vxr()
        
        offsets = []

        for variable, offset in zip(self.zvdrs, self.array_offsets):
            entry = {}
            entry['name'] = strip_null(variable.Name)
            entry['offset'] = offset
            offsets.append(entry)
            if show:
                print('Variable: %s has offset %d' % (offset['name'], offset))

        return offsets

## end of CDF class
 
def strip_null(text):
    """Strips null values from unpacked string bytes"""

    return text.decode("utf-8").split('\x00')[0]


def cdf_to_pds4(cdf_file, label_template):
    """Accepts a CDF file and a PDS4 label template and calls the  IGPP docgen tool
    to write a corresponding label file"""

    pass