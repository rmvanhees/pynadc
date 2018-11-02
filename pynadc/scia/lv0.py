"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to read (every byte of) Sciamachy level 0 data products

The SRON Sciamachy level 0 data set contains 48474 products from the period:
 18 June 2002 until 08 April 2012

The Sciamachy level 0 contains only 10 products of the same orbit: 19071,
 19624, 19667, 20831, 23990, 34010, 48428, 51208, 51209, 51210.

Nearly 400 products contain data corruption, which require safe-read.

Statistics on proc-stage
   1757 O
  46665 P
     52 S

Copyright (c) 2012-2018 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  Standard 3-clause BSD
"""
from operator import itemgetter
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
    consts['num_aux_bcp'] = 16
    consts['num_aux_pmtc_frame'] = 5
    consts['num_pmd_packets'] = 200
    consts['channel_pixels'] = 1024

    if key is None:
        return consts
    if key in consts:
        return consts[key]

    raise KeyError('level 0 constant {} is not defined'.format(key))


def check_dsr_in_states(mds, verbose=False, check=False):
    """
    This module combines L0 DSR per state ID based on parameter icu_time.
    """
    # combine L0 DSR on parameter icu_time
    # alternatively one could use parameter state_id
    _arr = mds['data_hdr']['icu_time']
    _arr = np.concatenate(([-1], _arr, [-1]))
    indx = np.where(np.diff(_arr) != 0)[0]
    num_dsr = np.diff(indx)
    icu_time = mds['data_hdr']['icu_time'][indx[:-1]]
    state_id = mds['data_hdr']['state_id'][indx[:-1]]
    if 'pmtc_frame' in mds.dtype.names:
        bcps = mds['pmtc_frame']['bcp']['bcps'][:, 0, 0]
    elif 'pmd_data' in mds.dtype.names:
        bcps = mds['pmd_data']['bcps'][:, 0]
    else:
        bcps = mds['pmtc_hdr']['bcps']
    if verbose:
        for ni in range(num_dsr.size):
            if ni+1 < num_dsr.size:
                diff_bcps = np.diff(bcps[indx[ni]:indx[ni+1]])
            if len(diff_bcps) > 1:
                print("# {:3d} state_{:02d} {:5d} {:4d}".format(
                    ni, state_id[ni], indx[ni], num_dsr[ni]),
                      icu_time[ni],
                      np.all(diff_bcps > 0),
                      np.all(diff_bcps == diff_bcps[0]))
            else:
                print("# {:3d} state_{:02d} {:5d} {:4d}".format(
                    ni, state_id[ni], indx[ni], num_dsr[ni]),
                      icu_time[ni],
                      np.all(diff_bcps > 0))

    if not check:
        return mds

    # Here we simply reject the shortest continuous set of measurements
    # with the same icu_time
    # Alternatively, one should reject duplicated DSR after reading the
    # whole DSR. For example, based on presence of data corruption
    _, inverse, count = np.unique(icu_time,
                                  return_inverse=True,
                                  return_counts=True)
    if np.any(count > 1):
        for ni in np.where(count > 1)[0]:
            indx_dbl = np.where(inverse == ni)[0]
            # print(ni, indx_dbl, icu_time[indx_dbl], num_dsr[indx_dbl])
            for reject in np.argsort(num_dsr[indx_dbl])[:-1]:
                start = indx[indx_dbl[reject]]
                end = start + num_dsr[indx_dbl[reject]]
                # print(ni, reject, start, end)
                mds['fep_hdr']['_quality'][start:end] = 0xFFFF

        print("# Info - rejected {} DSRs".format(
            np.sum(mds['fep_hdr']['_quality'] != 0)))

    return mds

def get_clus_def(det_mds):
    """
    determine cluster definition (same as clusDef in L1b states ADS)
    """
    from .hk import get_det_vis_pet, get_det_ir_pet

    mtbl_dtype = np.dtype([
        ('type_clus', 'u1'),
        ('num_clus', 'u1'),
        ('duration', 'u2'),
        ('num_info', 'u2'),
    ])

    clus_dtype = np.dtype([
        ('id', 'u1'),              # 1 <= id <= 64
        ('channel', 'u1'),         # 1 <= channel <= 8
        ('coaddf', 'u1'),
        ('type', 'u1'),            # coaddf == 1 ? 1 : 2
        ('start', 'u2'),           # 0 <= start < 1023
        ('length', 'u2'),          # 0 <= start < 1023
        ('intg', 'u2'),            # 16 * coaddf * pet
        ('n_read', 'u2'),
        ('pet', 'f4')
    ])

    bcps = 0
    clus_list = []
    for det in det_mds:
        first = True
        num_chan = det['pmtc_hdr']['num_chan']
        for chan in det['chan_data'][:num_chan]:
            chan_id = chan['hdr']['id_is_lu'] >> 4
            if first:
                bcps += chan['hdr']['bcps']
                first = False
            if chan_id < 6:
                pet = None
                pet_list, vir_chan_b = get_det_vis_pet(chan['hdr'])
                if isinstance(pet_list, float):
                    pet = pet_list
            else:
                vir_chan_b = 0
                pet = get_det_ir_pet(chan['hdr'])
                pet_list = None

            num_clus = chan['hdr']['clusters']
            for clus in chan['clus_hdr'][:num_clus]:
                clus_id = clus['id']
                coaddf = clus['coaddf']
                start = clus['start'] % 1024
                length = clus['length']
                if isinstance(pet_list, list):
                    if start >= vir_chan_b:
                        pet = pet_list[1]
                    else:
                        pet = pet_list[0]
                clus_list.append((chan_id, clus_id, start, length, coaddf, pet))

    # fill the output structure
    clus_set = sorted(set(clus_list), key=itemgetter(0, 2))
    clus_def = np.empty(len(clus_set), dtype=clus_dtype)
    for ni, clus in enumerate(clus_set):
        clus_def[ni]['id'] = ni + 1
        clus_def[ni]['channel'] = clus[0]
        clus_def[ni]['coaddf'] = clus[4]
        clus_def[ni]['type'] = min(2, clus[4])
        clus_def[ni]['start'] = clus[2]
        clus_def[ni]['length'] = clus[3]
        clus_def[ni]['intg'] = max(1, int(16 * clus[4] * clus[5]))
        clus_def[ni]['n_read'] = 0
        clus_def[ni]['pet'] = clus[5]

    # finally, add number of readouts
    for clus in clus_def:
        clus['n_read'] = clus_def['intg'].max() // clus['intg']
        # print(clus)

    mtbl = np.zeros(1, dtype=mtbl_dtype)
    mtbl['num_clus'] = len(clus_set)
    mtbl['duration'] = (det_mds[-1]['pmtc_hdr']['bcps'] // det_mds.size
                        * det_mds.size)
    mtbl['num_info'] = det_mds.size

    return mtbl, clus_def

# - Classes --------------------------------------
class File():
    """
    Class to read Sciamachy level 0 products
    """
    def __init__(self, flname, only_headers=False):
        """
        read whole product into memory: ascii headers and all DSRs
        """
        # initialize class attributes
        self.filename = flname
        self.sorted = {'det' : True,      # assume DSR packets are sorted
                       'aux' : True,
                       'pmd' : True}
        self.mph = {}
        self.sph = {}
        self.dsd = None
        self.info = None

        # check is file is compressed
        magic_dict = {
            "\x1f\x8b\x08": "gz",
            "\x42\x5a\x68": "bz2",
            "\xfd\x37\x7a\x58\x5a\x00": "xz"
        }
        for magic, _ in magic_dict.items():
            with open(flname, 'rb') as fp:
                file_magic = fp.read(len(magic))

            if file_magic == magic:
                raise SystemError("can not read compressed file")

        # read Main Product Header
        self.__get_mph__()

        # read Specific Product Header
        self.__get_sph__()
        if 'SPH_DESCRIPTOR' not in self.sph:
            raise ValueError('SPH_DESCRIPTOR not found in product header')
        if not self.sph['SPH_DESCRIPTOR'].startswith("SCI_NL__0P SPECIFIC"):
            raise ValueError('not a Sciamachy level 0 product')

        # read Data Set Descriptors
        self.__get_dsd__()

        # check file size
        if self.mph['TOT_SIZE'] != Path(flname).stat().st_size:
            raise SystemError('file {} incomplete'.format(flname))

        # read remainder of the file as info-records
        if not only_headers:
            self.__get_info__()

    # ----- generic data structures -------------------------
    @staticmethod
    def __mjd_envi():
        """
        Returns numpy-dtype definition for a mjd record
        """
        return np.dtype([
            ('days', '>i4'),
            ('secnds', '>u4'),
            ('musec', '>u4')
        ])

    def __fep_hdr(self):
        """
        Returns numpy-dtype definition for a front-end processor header
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
        Returns numpy-dtype definition for a packet header
        """
        return np.dtype([
            ('id', '>u2'),
            ('control', '>u2'),
            ('length', '>u2')
        ])

    @staticmethod
    def __data_hdr():
        """
        Returns numpy-dtype definition for a data-field header
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

    def ds_hdr_dtype(self):
        """
        Returns only the common part of DSR: auxiliary, detector or PMD
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr())
        ])

    # ----- detector data structures -------------------------
    @staticmethod
    def __det_pmtc_hdr():
        """
        Returns numpy-dtype definition for a pmtc header
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
        Returns numpy-dtype definition for a channel data header
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
    def __clus_hdr():
        """
        Returns numpy-dtype definition for a pixel data block,
           one per cluster read-out
        """
        return np.dtype([
            ('sync', '>u2'),
            ('block', '>u2'),
            ('id', 'u1'),
            ('coaddf', 'u1'),
            ('start', '>u2'),
            ('length', '>u2')
        ])

    def __chan_data(self):
        """
        Returns numpy-dtype definition for a channel data structure
        """
        return np.dtype([
            ('hdr', self.__chan_hdr()),
            ('clus_hdr', self.__clus_hdr(), (12)),  # theoretical maximum is 16
            ('clus_data', 'O', (12))
        ])

    def det_mds_dtype(self):
        """
        Returns numpy-dtype definition for a level 0 detector mds
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('pmtc_hdr', self.__det_pmtc_hdr()),
            ('chan_data', self.__chan_data(), (8))
        ])

    def chan_dtype(self):
        """
        Returns numpy-dtype definition for science channel data
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('icu_time', 'u4'),
            ('bcps', 'u2'),
            ('temp', 'f8'),
            ('coaddf', 'u1', (lv0_consts('channel_pixels'))),
            ('data', 'f8', (lv0_consts('channel_pixels')))
        ])

    # ----- auxiliary data structures -------------------------
    @staticmethod
    def __aux_pmtc_hdr():
        """
        Returns numpy-dtype definition for a pmtc header
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
        Returns numpy-dtype definition for a pmtc auxiliary frame
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
            ('bcp', aux_bcp_dtype, (lv0_consts('num_aux_bcp'))),
            ('bench_rad', '>u2'),
            ('bench_elv', '>u2'),
            ('bench_az', '>u2')
        ])

    def aux_mds_dtype(self):
        """
        Returns numpy-dtype definition for a level 0 auxiliary mds
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('pmtc_hdr', self.__aux_pmtc_hdr()),
            ('pmtc_frame', self.__pmtc_frame(),
             lv0_consts('num_aux_pmtc_frame'))
        ])

    # ----- PMD data structures -------------------------
    @staticmethod
    def __pmd_data():
        """
        Returns numpy-dtype definition for a PMD data packet
        """
        return np.dtype([
            ('sync', '>u2'),
            ('data', '>u2', (2, 7)),
            ('bcps', '>u2'),
            ('time', '>u2')
        ])

    def pmd_mds_dtype(self):
        """
        Returns numpy-dtype definition for a level 0 auxiliary mds
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('temp', '>u2'),
            ('pmd_data', self.__pmd_data(),
             lv0_consts('num_pmd_packets'))
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
        fp.seek(lv0_consts('mds_size'))     # skip MPH header

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

    def __get_info__(self):
        """
        read Sciamachy level 0 DSD records
        """
        # select DSD with name 'SCIAMACHY_SOURCE_PACKETS'
        dsd = None
        for dsd in self.dsd:
            if dsd['DS_NAME'] == 'SCIAMACHY_SOURCE_PACKETS':
                break

        # define info record which hold the generic headers, bcps
        # and a copy of the remaining bytes of a DSR
        info_dtype = np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('bcps', '>u2'),
            ('buff', 'O')
        ])

        # store all MDS data in these buffers
        self.info = np.empty(dsd['NUM_DSR'], dtype=info_dtype)

        # collect information about the level 0 measurement data in product
        num_det = num_aux = num_pmd = 0
        last_det_icu_time = last_aux_icu_time = last_pmd_icu_time = 0
        with open(self.filename, 'rb') as fp:
            ds_hdr_dtype = self.ds_hdr_dtype()
            fp.seek(dsd['DS_OFFSET'])
            for ni, ds_rec in enumerate(self.info):
                ds_hdr = np.fromfile(fp, dtype=ds_hdr_dtype, count=1)[0]

                # how much data do we still need to read
                num_bytes = self.bytes_left(ds_hdr, ds_hdr_dtype.itemsize)
                # check for header-data corruption
                if num_bytes < 0:
                    print("# Info - read {} of {} DSRs".format(
                        ni, dsd['NUM_DSR']))
                    break

                # copy read buffer
                for key in ds_hdr_dtype.names:
                    ds_rec[key] = ds_hdr[key]

                # set quality flag to zero
                ds_rec['fep_hdr']['_quality'] = 0

                ds_rec['data_hdr']['packet_type'] >>= 4
                ds_rec['data_hdr']['packet_type'] &= 0x3
                if ds_rec['data_hdr']['packet_type'] == 3 \
                   or ds_rec['fep_hdr']['length'] == 6813:
                    num_pmd += 1
                    offs_bcps = 32
                    if last_pmd_icu_time > ds_rec['data_hdr']['icu_time']:
                        self.sorted['pmd'] = False
                    last_pmd_icu_time = ds_rec['data_hdr']['icu_time']
                elif ds_rec['data_hdr']['packet_type'] == 2 \
                     or ds_rec['fep_hdr']['length'] == 1659:
                    num_aux += 1
                    offs_bcps = 20
                    if last_aux_icu_time > ds_rec['data_hdr']['icu_time']:
                        self.sorted['aux'] = False
                    last_aux_icu_time = ds_rec['data_hdr']['icu_time']
                else:
                    if ds_rec['data_hdr']['packet_type'] != 1:
                        if ds_rec['data_hdr']['length'] != 66:
                            print("# Warning - unknown packet type")
                            print("# * feb_hdr: ", ds_rec['fep_hdr'])
                            print("# * packted_hdr: ", ds_rec['packet_hdr'])
                            print("# * data_hdr: ", ds_rec['data_hdr'])
                        ds_rec['data_hdr']['packet_type'] = 1
                    num_det += 1
                    offs_bcps = 0
                    if last_det_icu_time > ds_rec['data_hdr']['icu_time']:
                        self.sorted['det'] = False
                    last_det_icu_time = ds_rec['data_hdr']['icu_time']

                # read remainder of DSR
                ds_rec['buff'] = fp.read(num_bytes)

                # read BCPS
                ds_rec['bcps'] = np.frombuffer(
                    ds_rec['buff'], '>u2', count=1, offset=offs_bcps)

                # if ds_rec['data_hdr']['packet_type'] == 1:
                #    offs = ds_rec['data_hdr']['icu_time'] - 4.19e9
                #    print(ni, ds_rec['data_hdr']['state_id'],
                #          last_det_icu_time,
                #          ds_rec['data_hdr']['icu_time'],
                #          offs + ds_rec['bcps'] / 16)

        print('# Info - number of DSRs (sorted={}): {:4d} {:3d} {:3d}'.format(
            self.sorted['det'] & self.sorted['aux'] & self.sorted['pmd'],
            num_det, num_aux, num_pmd))

    def bytes_left(self, mds, read_sofar=0):
        """
        Returns number to be read from a MDS record
        """
        size = (mds['fep_hdr']['length'] + self.__mjd_envi().itemsize
                + self.__fep_hdr().itemsize + self.__packet_hdr().itemsize + 1)
        size -= read_sofar

        return size

    def __read_det_raw(self, ds_rec, ni, det_mds):
        """
        read detector DSR without any checks
        """
        det = det_mds[ni]

        # copy fixed part of the detector MDS
        offs = 0
        for key in self.ds_hdr_dtype().names:
            det[key] = ds_rec[key]

        # read detector specific part from buffer
        det['pmtc_hdr'] = np.frombuffer(ds_rec['buff'],
                                        dtype=self.__det_pmtc_hdr(),
                                        count=1,
                                        offset=offs)
        offs += self.__det_pmtc_hdr().itemsize
        det['pmtc_hdr']['num_chan'] &= 0xF

        # read channel data blocks
        channel = det['chan_data']
        for nch in range(det['pmtc_hdr']['num_chan']):
            channel['hdr'][nch] = np.frombuffer(
                ds_rec['buff'],
                dtype=self.__chan_hdr(),
                count=1,
                offset=offs)
            offs += self.__chan_hdr().itemsize

            # read cluster data
            hdr = channel['clus_hdr'][nch]
            buff = channel['clus_data'][nch]
            for ncl in range(channel['hdr']['clusters'][nch]):
                hdr[ncl] = np.frombuffer(ds_rec['buff'],
                                         dtype=self.__clus_hdr(),
                                         count=1,
                                         offset=offs)
                offs += self.__clus_hdr().itemsize

                if hdr['coaddf'][ncl] == 1:
                    nbytes = 2 * hdr['length'][ncl]
                    buff[ncl] = np.frombuffer(ds_rec['buff'],
                                              dtype='>u2',
                                              count=hdr['length'][ncl],
                                              offset=offs)
                else:
                    nbytes = 3 * hdr['length'][ncl]
                    buff[ncl] = np.frombuffer(ds_rec['buff'],
                                              dtype='u1',
                                              count=nbytes,
                                              offset=offs)
                    if (nbytes % 2) == 1:
                        nbytes += 1
                offs += nbytes

        return det

    def __read_det_safe(self, ds_rec, ni, det_mds):
        """
        read detector DSR with sanity checks
        """
        det = det_mds[ni]

        # copy fixed part of the detector MDS
        offs = 0
        for key in self.ds_hdr_dtype().names:
            det[key] = ds_rec[key]

        # read detector specific part from buffer
        det['pmtc_hdr'] = np.frombuffer(ds_rec['buff'],
                                        dtype=self.__det_pmtc_hdr(),
                                        count=1,
                                        offset=offs)
        offs += self.__det_pmtc_hdr().itemsize
        det['pmtc_hdr']['num_chan'] &= 0xF

        # read channel data blocks
        channel = det['chan_data']
        for nch in range(det['pmtc_hdr']['num_chan']):
            if offs == len(ds_rec['buff']):
                det['pmtc_hdr']['num_chan'] = nch
                break

            channel['hdr'][nch] = np.frombuffer(
                ds_rec['buff'],
                dtype=self.__chan_hdr(),
                count=1,
                offset=offs)
            offs += self.__chan_hdr().itemsize
            # print(ni, nch, ds_rec['fep_hdr']['crc_errs'],
            #      det['pmtc_hdr']['num_chan'],
            #      channel['hdr']['sync'][nch],
            #      channel['hdr']['clusters'][nch],
            #      offs, len(ds_rec['buff']))
            channel['hdr']['clusters'][nch] &= 0xF
            if channel['hdr']['sync'][nch] != 0xAAAA:
                print("# Warning - channel-sync corruption", ni, nch)
                det['pmtc_hdr']['num_chan'] = nch
                det['fep_hdr']['_quality'] |= 0x1
                break

            # read cluster data
            hdr = channel['clus_hdr'][nch]
            buff = channel['clus_data'][nch]
            for ncl in range(channel['hdr']['clusters'][nch]):
                if offs == len(ds_rec['buff']):
                    channel['hdr']['clusters'][nch] = ncl
                    break

                hdr[ncl] = np.frombuffer(ds_rec['buff'],
                                         dtype=self.__clus_hdr(),
                                         count=1,
                                         offset=offs)
                offs += self.__clus_hdr().itemsize
                # print(ni, nch, ncl, ds_rec['fep_hdr']['crc_errs'],
                #      det['pmtc_hdr']['num_chan'],
                #      channel['hdr']['sync'][nch],
                #      channel['hdr']['clusters'][nch],
                #      hdr['sync'][ncl], hdr['start'][ncl],
                #      hdr['length'][ncl], hdr['coaddf'][ncl],
                #      offs, len(ds_rec['buff']))
                if hdr['sync'][ncl] != 0xBBBB:
                    print("# Warning - cluster-sync corruption", ni, nch, ncl)
                    channel['hdr']['clusters'][nch] = ncl
                    det['fep_hdr']['_quality'] |= 0x2
                    break

                # mask bit-flips in cluster parameters start and length
                hdr['start'][ncl] &= 0x1FFF
                hdr['length'][ncl] &= 0x7FF

                # check coadding factor
                bytes_left = len(ds_rec['buff']) - offs
                if hdr['coaddf'][ncl] != 1 \
                   and 2 * hdr['length'][ncl] == bytes_left:
                    hdr['coaddf'][ncl] = 1

                if hdr['coaddf'][ncl] == 1:
                    nbytes = 2 * hdr['length'][ncl]
                    if nbytes > bytes_left:
                        print("# Warning - cluster-size corruption",
                              ni, nch, ncl)
                        channel['hdr']['clusters'][nch] = ncl
                        buff[ncl] = None
                        det['fep_hdr']['_quality'] |= 0x4
                        break
                    buff[ncl] = np.frombuffer(ds_rec['buff'],
                                              dtype='>u2',
                                              count=hdr['length'][ncl],
                                              offset=offs)[0]
                else:
                    nbytes = 3 * hdr['length'][ncl]
                    if nbytes > bytes_left:
                        print("# Warning - cluster-size corruption",
                              ni, nch, ncl)
                        channel['hdr']['clusters'][nch] = ncl
                        buff[ncl] = None
                        det['fep_hdr']['_quality'] |= 0x4
                        break
                    buff[ncl] = np.frombuffer(ds_rec['buff'],
                                              dtype='u1',
                                              count=nbytes,
                                              offset=offs)[0]
                    if (nbytes % 2) == 1:
                        nbytes += 1
                offs += nbytes
            else:
                continue
            # only excecuted if a break occurred during read of
            # cluster data
            det['pmtc_hdr']['num_chan'] = nch
            break

        return det

    # repair SCIAMACHY_SOURCE_PACKETS
    def repair_info(self):
        """
        Repairs chronological order of info records

        Description
        -----------
        Checks attribute 'sorted', nothing is done when DSR's are sorted
        1) repair corrupted icu_time values within a state execution
        2) put state executions in chronological order (based on icu_time)
           and remove repeated states or partly repeated states in product
        """
        if self.info is None:
            self.__get_info__()

        if self.sorted['det'] & self.sorted['aux'] & self.sorted['pmd']:
            return

        info_list = ()
        for name, _id in (['det', 1], ['aux', 2], ['pmd', 3]):
            indx = np.where(self.info['data_hdr']['packet_type'] == _id)[0]
            info = self.info[indx]

            uniq, inverse, counts = np.unique(
                info['data_hdr']['icu_time'],
                return_counts=True, return_inverse=True)

            # 1) repair corrupted icu_time values
            for ii in np.where(counts == 1)[0]:
                indx = np.where(inverse == ii)[0][0]
                if indx > 0 and indx + 1 < info.size:
                    if (info['data_hdr']['icu_time'][indx-1]
                            == info['data_hdr']['icu_time'][indx+1]) \
                        and (info['data_hdr']['state_id'][indx-1]
                             == info['data_hdr']['state_id'][indx]
                             == info['data_hdr']['state_id'][indx+1]) \
                        and (info['bcps'][indx-1]
                             < info['bcps'][indx]
                             < info['bcps'][indx+1]):
                        print("# Info - icu_time of {}_mds[{}] fixed".format(
                            name, indx))
                        info['data_hdr']['icu_time'][indx] = \
                                        info['data_hdr']['icu_time'][indx-1]
                    else:
                        print("# Warning - can not fix {}_mds[{}]".format(
                            name, indx))
                else:
                    print("# Warning - can not fix {}_mds[{}]".format(
                        name, indx))

            # 2) put state executions in chronological order
            #    and remove repeated states or partly repeated states in product
            if np.any(np.diff(info['data_hdr']['icu_time'].astype(int)) < 0):
                uniq, inverse = np.unique(
                    info['data_hdr']['icu_time'], return_inverse=True)

                for ii in range(uniq.size):
                    indx = np.where(inverse == ii)[0]
                    blocks = np.where(np.diff(np.concatenate(([-1],
                                                              indx,
                                                              [-1]))) != 1)[0]
                    if blocks.size == 1:
                        info_list += (info[indx],)
                        continue

                    num = np.argmax(np.diff(blocks))
                    if np.diff(blocks)[num] == 1:
                        print("# Warning - rejected {}_mds: {}".format(
                            name, indx))
                        continue

                    info_list += (info[indx[blocks[num]:blocks[num+1]]],)
            else:
                info_list += (info,)

        # combine all detector, auxiliary and pmd DSRs
        self.info = np.concatenate(info_list)

    # read SCIAMACHY_SOURCE_PACKETS
    def get_mds(self, state_id=None):
        """
        read Sciamachy level 0 MDS records into numpy compound-arrays

        Parameters
        ----------
        state_id : list
         read only DSRs of selected states

        Returns
        -------
        numpy array

        Description
        -----------
        First all DSRs are read from disk, only the common data-headers of the
        level 0 detector, auxiliary and PMD packets are stored in structured
        numpy arrays. The remainder of the data packets are read as byte arrays
        from disk according to the DSR size specified in the FEP header.

        In a second run, all information stored in the DSRs are stored in
        structured numpy arrays. The auxiliary and PMD DSRs have a fixed size,
        and can be copied using the function numpy.frombuffer(). The detector
        DSRs have to be read dynamically, as their size can vary based on the
        instrument settings (defined by the state ID). The shape and size of
        the detector DSR is defined by the number of channels, the number of
        clusters per channel, the number of pixels per cluster and the
        co-adding factor of the read-outs. All these parameters are stored in
        the data headers of each DSR. Any of these parameters can be
        corrupted, which result in a incorrect interpretation of the DSR.
        Only a very small faction of the data is affected by data corruption,
        reported in the FEP header parameters CRC errors and RS errors. When,
        the interpretation of the data streams fails, we re-read the DSR with
        a much slower fail-safe interpretation, this routine performes various
        sanity checks and will ingnore data of a DSR after a data coruption,
        but will interpret any remaining DSRs.
        """
        if self.info is None:
            self.__get_info__()

        # ----- read level 0 detector data packets -----
        # possible variants: raw, safe, clus_def
        indx_det = np.where(self.info['data_hdr']['packet_type'] == 1)[0]
        if indx_det.size > 0:
            det_mds = np.empty(len(indx_det), dtype=self.det_mds_dtype())

            ni = 0
            for ds_rec in self.info[indx_det]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                try:
                    det_mds[ni] = self.__read_det_raw(ds_rec, ni, det_mds)
                except (IndexError, ValueError, RuntimeError):
                    det_mds[ni] = self.__read_det_safe(ds_rec, ni, det_mds)
                ni += 1

            det_mds = det_mds[0:ni]
            print("# Info - read {} detector mds".format(ni))

        # ----- read level 0 auxiliary data packets -----
        indx_aux = np.where(self.info['data_hdr']['packet_type'] == 2)[0]
        if indx_aux.size > 0:
            aux_mds = np.empty(len(indx_aux), dtype=self.aux_mds_dtype())

            ni = 0
            for ds_rec in self.info[indx_aux]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                # copy fixed part of the auxiliary MDS
                aux = aux_mds[ni]
                for key in self.ds_hdr_dtype().names:
                    aux[key] = ds_rec[key]

                # read auxiliary specific part from buffer
                aux['pmtc_hdr'] = np.frombuffer(
                    ds_rec['buff'],
                    dtype=self.__aux_pmtc_hdr(),
                    count=1,
                    offset=0)

                aux['pmtc_frame'] = np.frombuffer(
                    ds_rec['buff'],
                    dtype=self.__pmtc_frame(),
                    count=lv0_consts('num_aux_pmtc_frame'),
                    offset=self.__aux_pmtc_hdr().itemsize)
                ni += 1

            aux_mds = aux_mds[0:ni]
            print("# Info - read {} auxiliary mds".format(ni))

        # ----- read level 0 PMD data packets -----
        indx_pmd = np.where(self.info['data_hdr']['packet_type'] == 3)[0]
        if indx_pmd.size > 0:
            pmd_mds = np.empty(len(indx_pmd), dtype=self.pmd_mds_dtype())

            ni = 0
            for ds_rec in self.info[indx_pmd]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                # copy fixed part of the PMD MDS
                pmd = pmd_mds[ni]
                for key in self.ds_hdr_dtype().names:
                    pmd[key] = ds_rec[key]

                # read PMD specific part from buffer
                pmd['temp'] = np.frombuffer(
                    ds_rec['buff'],
                    dtype='>u2',
                    count=1,
                    offset=0)

                pmd['pmd_data'] = np.frombuffer(
                    ds_rec['buff'],
                    dtype=self.__pmd_data(),
                    count=lv0_consts('num_pmd_packets'),
                    offset=2)
                ni += 1

            pmd_mds = pmd_mds[0:ni]
            print("# Info - read {} PMD mds".format(ni))

        return (det_mds, aux_mds, pmd_mds)

    def get_channel(self, state_id, chan_id):
        """
        combines readouts of one science channel for a given state ID
        """
        from .hk import get_det_temp

        (det_mds, _, _) = self.get_mds(state_id)
        if det_mds.size == 0:
            return None
        chan = np.empty(det_mds.size, dtype=self.chan_dtype())
        chan['data'][...] = np.nan

        for ni, dsr in enumerate(det_mds):
            chan['isp'][ni] = dsr['isp']
            chan['icu_time'][ni] = dsr['data_hdr']['icu_time']
            chan['bcps'][ni] = dsr['pmtc_hdr']['bcps']
            for nch in range(dsr['pmtc_hdr']['num_chan']):
                chan_data = dsr['chan_data'][nch]
                if chan_data['hdr']['id_is_lu'] >> 4 != chan_id:
                    continue

                # convert raw temperature count to Kelvin
                chan['temp'][ni] = get_det_temp(chan_id,
                                                chan_data['hdr']['temp'])
                for ncl in range(chan_data['hdr']['clusters']):
                    ii = chan_data['clus_hdr']['start'][ncl] % 1024
                    jj = ii + chan_data['clus_hdr']['length'][ncl]
                    chan['coaddf'][ni][ii:jj] = \
                        chan_data['clus_hdr']['coaddf'][ncl]
                    if chan_data['clus_hdr']['coaddf'][ncl] == 1:
                        chan['data'][ni][ii:jj] = chan_data['clus_data'][ncl]
                    else:
                        dim = chan_data['clus_hdr']['length'][ncl]
                        buffer = np.zeros(dim * 4, dtype='u1').reshape(dim, 4)
                        buffer[:, 1:] = np.array(
                            chan_data['clus_data'][ncl]).reshape(dim, 3)
                        chan['data'][ni][ii:jj] = np.frombuffer(
                            buffer.tobytes(), dtype='>u4')

        # remove empty entries
        indx = np.where(~np.all(np.isnan(chan['data']), axis=1))[0]
        return chan[indx]
