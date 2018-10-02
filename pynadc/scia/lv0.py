"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to read Sciamachy level 0 data products

Copyright (c) 2012-2018 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  Standard 3-clause BSD
"""
from pathlib import Path

import numpy as np

# - global parameters ------------------------------


# - local functions --------------------------------
def lv0_consts(key=None):
    """
    defines consts used while reading Sciamachy level 0 data
    """
    consts = {}
    consts['mds_size'] = 1247
    consts['num_lv0_aux_bcp'] = 16
    consts['num_lv0_aux_pmtc_frame'] = 5
    consts['num_lv0_pmd_packets'] = 200

    if key is None:
        return consts
    if key in consts:
        return consts[key]
    
    raise KeyError('level 0 constant {} is not defined'.format(key))

# - Classes --------------------------------------
class File():
    """
    Class to read Sciamachy level 0 products
    """
    def __init__(self, flname):
        """
        """
        # initialize class attributes
        self.filename = flname
        self.mph = {}
        self.sph = {}
        self.dsd = None

        # check is file is compressed
        magic_dict = {
            "\x1f\x8b\x08": "gz",
            "\x42\x5a\x68": "bz2",
            "\xfd\x37\x7a\x58\x5a\x00": "xz"
        }
        for magic, filetype in magic_dict.items():
            with open(flname, 'rb') as fp:
                file_magic = fp.read(len(magic))

            if file_magic == magic:
                raise SystemError('file is compressed with {}'.format(filetype))

        # read Main Product Header
        self.__get_mph__()
        # read Specific Product Header
        self.__get_sph__()
        # read Data Set Descriptors
        self.__get_dsd__()

        # check file size
        if self.mph['TOT_SIZE'] != Path(flname).stat().st_size:
            raise SystemError('file {} incomplete'.format(flname))

    # ----- generic data structures -------------------------    
    @staticmethod
    def __mjd_envi():
        """
        return numpy-dtype definition for a mjd record
        """
        return np.dtype([
            ('days', '>i4'),
            ('secnds', '>u4'),
            ('musec', '>u4')
        ])

    def __fep_hdr(self):
        """
        return numpy-dtype definition for a front-end processor header
        """
        return np.dtype([
            ('mjd', self.__mjd_envi()),
            ('length', '>u2'),
            ('crc_errs', '>u2'),
            ('rs_errs', '>u2'),
            ('_quality', 'u2')         # spare
        ])

    @staticmethod
    def __packet_hdr():
        """
        return numpy-dtype definition for a packet header
        """
        return np.dtype([
            ('id', '>u2'),
            ('control', '>u2'),
            ('length', '>u2')
        ])

    @staticmethod
    def __data_hdr():
        """
        return numpy-dtype definition for a data-field header
        """
        return np.dtype([
            ('length', '>u2'),
            ('category', 'u1'),
            ('state_id', 'u1'),
            ('icu_time', '>u4'),
            ('rdv', '>u2'),
            ('packet_type', 'u1'),
            ('overflow', 'u1')
        ])

    def info_mds_dtype(self):
        """
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep', self.__fep_hdr()),
            ('packet', self.__packet_hdr()),
            ('data', self.__data_hdr())
        ])

    # ----- detector data structures -------------------------    
    @staticmethod
    def __det_pmtc_hdr():
        """
        return numpy-dtype definition for a pmtc header
        """
        return np.dtype([
            ('bcps', '>u2'),
            ('pmtc_1', '>u2'),
            ('scanner_mode', '>u2'),
            ('az_param', '>u4'),
            ('elev_param', '>u4'),
            ('factors', 'u1', (6)),
            ('orbit_vector', '>i4', (8)),
            ('num_chan', '>u2')
        ])

    @staticmethod
    def __chan_hdr():
        """
        return numpy-dtype definition for a channel data header
        """
        return np.dtype([
            ('sync', '>u2'),
            ('id_is_lu', 'u1'),     # lu:2, is:2, id:4
            ('clusters', 'u1'),
            ('bcps', '>u2'),
            ('command', '>u4'),
            ('ratio', 'u1'),        # ratio:5, status:3
            ('frame', 'u1'),
            ('bias', '>u2'),
            ('temp', '>u2')
        ])

    @staticmethod
    def __clus_data():
        """
        return numpy-dtype definition for a pixel data block,
           one per cluster read-out
        """
        return np.dtype([
            ('sync', '>u2'),
            ('block', '>u2'),
            ('id', 'u1'),
            ('coaddf', 'u1'),
            ('start', '>u2'),
            ('length', '>u2'),
            ('offset', 'i4')
        ])

    def __chan_data(self):
        """
        return numpy-dtype definition for a channel data structure
        """
        return np.dtype([
            ('hdr', self.__chan_hdr()),
            ('data', self.__clus_data(), (12))  # theoretical maximum is 16
        ])
    
    def det_mds_dtype(self, header=False):
        """
        return numpy-dtype definition for a level 0 detector mds
        """
        if header:
            return np.dtype([
                ('isp', self.__mjd_envi()),
                ('fep_hdr', self.__fep_hdr()),
                ('packet_hdr', self.__packet_hdr()),
                ('data_hdr', self.__data_hdr()),
                ('pmtc_hdr', self.__det_pmtc_hdr())
            ])

        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('pmtc_hdr', self.__det_pmtc_hdr()),
            ('chan_data', self.__chan_data(), (8))
       ])

    # ----- auxiliary data structures -------------------------    
    @staticmethod
    def __aux_pmtc_hdr():
        """
        return numpy-dtype definition for a pmtc header
        """
        return np.dtype([
            ('pmtc_1', '>u2'),
            ('scanner_mode', '>u2'),
            ('az_param', '>u4'),
            ('elev_param', '>u4'),
            ('factors', 'u1', (6))
        ])

    @staticmethod
    def __pmtc_frame():
        """
        return numpy-dtype definition for a pmtc auxiliary frame
        """
        aux_bcp_dtype = np.dtype([
            ('sync', '>u2'),
            ('bcps', '>u2'),
            ('flags', '>u2'),
            ('encode_cntr', 'u1', (6)),
            ('azi_cntr_error', '>u2'),
            ('ele_cntr_error', '>u2'),
            ('azi_scan_error', '>u2'),
            ('ele_scan_error', '>u2')
        ])

        return np.dtype([
            ('bcp', aux_bcp_dtype, (lv0_consts('num_lv0_aux_bcp'))),
            ('bench_rad', '>u2'),
            ('bench_elv', '>u2'),
            ('bench_az', '>u2')
        ])

    def aux_mds_dtype(self):
        """
        return numpy-dtype definition for a level 0 auxiliary mds
        """        
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('pmtc_hdr', self.__aux_pmtc_hdr()),
            ('pmtc_frame', self.__pmtc_frame(),
             lv0_consts('num_lv0_aux_pmtc_frame'))
       ])    

    # ----- PMD data structures -------------------------    
    @staticmethod
    def __pmd_data():
        """
        return numpy-dtype definition for a PMD data packet
        """
        return np.dtype([
            ('sync', '>u2'),
            ('data', '>u2', (2, 7)),
            ('bcps', '>u2'),
            ('time', '>u2')
        ])

    def pmd_mds_dtype(self):
        """
        return numpy-dtype definition for a level 0 auxiliary mds
        """        
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('temp', '>u2'),
            ('pmd_data', self.__pmd_data(),
             lv0_consts('num_lv0_pmd_packets'))
       ])    
    
    # ----- read routines -------------------------    
    def __get_mph__(self):
        """
        read Sciamachy level 0 MPH header
        """
        fp = open(self.filename, 'rt', encoding='latin-1')

        for line in fp:
            words = line[:-1].split('=')
            # only process (key,value)
            if len(words) != 2:
                continue

            # end of MPH header
            if words[0] == 'SPH_DESCRIPTOR':
                break

            if words[1][0] == '\"':
                self.mph[words[0]] = words[1].strip('\"').rstrip()
            elif len(words[1]) == 1:
                self.mph[words[0]] = words[1]
            elif words[1].find('<s>') > 0:
                self.mph[words[0]] = float(words[1][0:words[1].find('<s>')])
            elif words[1].find('<m>') > 0:
                self.mph[words[0]] = float(words[1][0:words[1].find('<m>')])
            elif words[1].find('<m/s>') > 0:
                self.mph[words[0]] = float(words[1][0:words[1].find('<m/s>')])
            elif words[1].find('<ps>') > 0:
                self.mph[words[0]] = int(words[1][0:words[1].find('<ps>')])
            elif words[1].find('<bytes>') > 0:
                self.mph[words[0]] = int(words[1][0:words[1].find('<bytes>')])
            else:
                self.mph[words[0]] = int(words[1])

        fp.close()

    def __get_sph__(self):
        """
        read Sciamachy level 0 SPH header
        """
        fp = open(self.filename, 'rt', encoding='latin-1')
        fp.seek(lv0_consts('mds_size')) # skip MPH header

        for line in fp:
            words = line.split('=')
            # only process (key,value)
            if len(words) != 2:
                continue
            # end of SPH header
            if words[0] == 'DS_NAME':
                break

            if words[1][0] == '\"':
                self.sph[words[0]] = words[1].strip("\"").rstrip()
            elif len(words[1]) == 1:
                self.sph[words[0]] = words[1]
            elif words[1].find('<10-6degN>') > 0:
                self.sph[words[0]] = int(
                    words[1][0:words[1].find('<10-6degN>')]) * 1e-6
            elif words[1].find('<10-6degE>') > 0:
                self.sph[words[0]] = int(
                    words[1][0:words[1].find('<10-6degE>')]) * 1e-6
            elif words[1].find('<deg>') > 0:
                self.sph[words[0]] = float(words[1][0:words[1].find('<deg>')])
            elif words[1].find('<%>') > 0:
                self.sph[words[0]] = float(words[1][0:words[1].find('<%>')])
            elif words[1].find('<>') > 0:
                self.sph[words[0]] = float(words[1][0:words[1].find('<>')])
            else:
                self.sph[words[0]] = int(words[1])

        fp.close()

    def __get_dsd__(self):
        """
        read Sciamachy level 0 DSD records
        """
        num_dsd = 0
        self.dsd = [{}]

        fp = open(self.filename, 'rt', encoding='latin-1')
        # skip headers MPH & SPH
        fp.seek(lv0_consts('mds_size') + self.mph['SPH_SIZE']
                - self.mph['NUM_DSD'] * self.mph['DSD_SIZE'])

        for line in fp:
            words = line[:-1].split('=')
            # only process (key,value)
            if len(words) != 2:
                continue

            if words[1][0] == '\"':
                self.dsd[num_dsd][words[0]] = words[1].strip('\"').rstrip()
            elif len(words[1]) == 1:
                self.dsd[num_dsd][words[0]] = words[1]
            elif words[1].find('<bytes>') > 0:
                self.dsd[num_dsd][words[0]] = \
                    int(words[1][0:words[1].find('<bytes>')])
            else:
                self.dsd[num_dsd][words[0]] = int(words[1])

            # end of DSD header
            if words[0] == 'DSR_SIZE':
                num_dsd += 1
                if num_dsd+1 == self.mph['NUM_DSD']:
                    break
                else:
                    self.dsd.append({})

        fp.close()

    def bytes_left(self, mds, read_sofar=0):
        """
        """
        size = (mds['fep']['length'] + self.__mjd_envi().itemsize
                + self.__fep_hdr().itemsize + self.__packet_hdr().itemsize + 1)
        size -= read_sofar

        return size
        
    # read SCIAMACHY_SOURCE_PACKETS
    def get_mds(self, stateID=None, packetID=None, packetType=None):
        """
        read Sciamachy level 0 MDS records
        """
        num_aux = 0
        num_det = 0
        num_pmd = 0

        # select DSD with name 'SCIAMACHY_SOURCE_PACKETS'
        dsd = None
        for dsd in self.dsd:
            if dsd['DS_NAME'] == 'SCIAMACHY_SOURCE_PACKETS':
                break

        info_mds = np.empty(dsd['NUM_DSR'], dtype=self.info_mds_dtype())

        # collect information about the level 0 measurement data in product
        with open(self.filename, 'rb') as fp:
            info_mds_dtype = self.info_mds_dtype()
            fp.seek(dsd['DS_OFFSET'])
            for ni in range(dsd['NUM_DSR']):
                mds = np.fromfile(fp, dtype=info_mds_dtype, count=1)[0]
                info_mds[ni] = mds
                packet_type = mds['data']['packet_type'] >> 4
                if packet_type == 1:
                    num_det += 1
                elif packet_type == 2:
                    num_aux +=1
                elif packet_type == 3:
                    num_pmd += 1
                else:
                    raise ValueError(
                        'unknown packet type {}'.format(packet_type))

                fp.seek(self.bytes_left(mds, info_mds_dtype.itemsize), 1)

        print(num_aux, num_det, num_pmd)

        # read level 0 detector data packets
        det_mds = np.empty(num_det, dtype=self.det_mds_dtype())
        with open(self.filename, 'rb') as fp:
            det_hdr_dtype = self.det_mds_dtype(header=True)
            fp.seek(dsd['DS_OFFSET'])

            ni = 0
            for info_rec in info_mds:
                packet_type = info_rec['data']['packet_type'] >> 4
                if packet_type != 1:
                    fp.seek(self.bytes_left(info_rec), 1)
                    continue

                # read fixed part of the detectror MDS
                hdr = np.fromfile(fp, dtype=det_hdr_dtype, count=1)[0]
                for key in hdr.dtype.names:
                    det_mds[key][ni] = hdr[key]

                # read channel data blocks
                chan_data = det_mds['chan_data'][ni]
                for nch in range(hdr['pmtc_hdr']['num_chan']):
                    chan_data['hdr'][nch] = np.fromfile(
                        fp, dtype=self.__chan_hdr(), count=1)[0]

                    clus = chan_data['data'][nch]
                    for ncl in range(chan_data['hdr']['clusters'][nch]):
                        clus['sync'][ncl] = np.fromfile(
                            fp, dtype='>u2', count=1)
                        clus['block'][ncl] = np.fromfile(
                            fp, dtype='>u2', count=1)
                        clus['id'][ncl] = np.fromfile(
                            fp, dtype='u1', count=1)
                        clus['coaddf'][ncl] = np.fromfile(
                            fp, dtype='u1', count=1)
                        clus['start'][ncl] = np.fromfile(
                            fp, dtype='>u2',  count=1)
                        clus['length'][ncl] = np.fromfile(
                            fp, dtype='>u2', count=1)
                        clus['offset'][ncl] = fp.tell()
                        
                        if clus[ncl]['coaddf'] == 1:
                            nbytes = 2 * clus['length'][ncl]
                        else:
                            nbytes = 3 * clus['length'][ncl]
                            if (clus['length'][ncl] % 2) == 1:
                                nbytes += 1
                        fp.seek(nbytes, 1)
                ni += 1
        print('read {} detector mds'.format(ni))

        # read level 0 auxiliary data packets
        aux_mds = np.empty(num_aux, dtype=self.aux_mds_dtype())
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])

            ni = 0
            for info_rec in info_mds:
                packet_type = info_rec['data']['packet_type'] >> 4
                if packet_type != 2:
                    fp.seek(self.bytes_left(info_rec), 1)
                    continue

                aux_mds[ni] = np.fromfile(fp, dtype=self.aux_mds_dtype(),
                                          count=1)[0]
                ni += 1
        print('read {} auxiliary mds'.format(ni))

        # read level 0 PMD data packets
        pmd_mds = np.empty(num_pmd, dtype=self.pmd_mds_dtype())
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])

            ni = 0
            for info_rec in info_mds:
                packet_type = info_rec['data']['packet_type'] >> 4
                if packet_type != 3:
                    fp.seek(self.bytes_left(info_rec), 1)
                    continue

                pmd_mds[ni] = np.fromfile(fp, dtype=self.pmd_mds_dtype(),
                                          count=1)[0]
                ni += 1
        print('read {} PMD mds'.format(ni))
                
        return (det_mds, aux_mds, pmd_mds)

