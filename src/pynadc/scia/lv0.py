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

Copyright (c) 2012-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from pathlib import Path

import numpy as np

from .hk import get_det_temp, mjd_to_datetime


# - global parameters ------------------------------


# - local functions --------------------------------
def lv0_consts(key=None):
    """
    defines consts used while reading Sciamachy level 0 data
    """
    consts = {}
    consts['mph_size'] = 1247
    consts['num_aux_bcp'] = 16
    consts['num_aux_pmtc_frame'] = 5
    consts['num_pmd_packets'] = 200
    consts['channel_pixels'] = 1024

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
    def __init__(self, flname, only_headers=False):
        """
        read whole product into memory: ascii headers and all ISPs
        """
        # initialize class attributes
        self.filename = flname
        self.sorted = {'det': True,      # assume ISP packets are sorted
                       'aux': True,
                       'pmd': True}
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
            ('mjd', self.__mjd_envi()),   # time of reception ground station
            ('length', '>u2'),            # length ISP (- 7 bytyes)
            ('crc_errs', '>u2'),          # CRS errors
            ('rs_errs', '>u2'),           # reed-solomon corrections
            ('_quality', 'u2')            # spare (zero=good)
        ])

    @staticmethod
    def __packet_hdr():
        """
        Returns numpy-dtype definition for a packet header
        """
        return np.dtype([
            ('id', '>u2'),                # packet identifier [1, 2, 3]
            ('control', '>u2'),           # packet sequence control
            ('length', '>u2')             # packet length (= FEP['length'])
        ])

    @staticmethod
    def __data_hdr():
        """
        Returns numpy-dtype definition for a data-field header
        """
        return np.dtype([
            ('length', '>u2'),            # data field header length
            ('category', 'u1'),           # measurement category
            ('state_id', 'u1'),           # instrument state identifier
            ('icu_time', '>u4'),          # ICU on-board time
            ('rdv', '>u2'),               # contains: HSM, ATC table, Config ID
            ('packet_type', 'u1'),        # packet identifier (det/aux/pmd)
            ('overflow', 'u1')            # buffer overflow indicator
        ])

    def ds_hdr_dtype(self):
        """
        Returns only the common part of L0 ISP: auxiliary, detector or PMD
        """
        return np.dtype([
            ('mjd', self.__mjd_envi()),
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

    def det_isp_dtype(self):
        """
        Returns numpy-dtype definition for a level 0 detector ISP
        """
        return np.dtype([
            ('mjd', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('pmtc_hdr', self.__det_pmtc_hdr()),
            ('chan_data', self.__chan_data(), (8))
        ])

    @staticmethod
    def chan_dtype():
        """
        Returns numpy-dtype definition for science channel data
        """
        return np.dtype([
            ('time', 'datetime64[us]'),
            ('icu_time', 'u4'),
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

    def aux_isp_dtype(self):
        """
        Returns numpy-dtype definition for a level 0 auxiliary ISP
        """
        return np.dtype([
            ('mjd', self.__mjd_envi()),
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

    def pmd_isp_dtype(self):
        """
        Returns numpy-dtype definition for a level 0 auxiliary ISP
        """
        return np.dtype([
            ('mjd', self.__mjd_envi()),
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
        def strip_unit(mystr: str, key: str) -> str:
            indx = mystr.find(key)
            if indx != -1:
                return mystr[0:indx]
            return None

        with open(self.filename, 'rt', encoding='latin-1') as fp:
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
                elif words[1].find('>') == -1:
                    self.mph[words[0]] = int(words[1])
                else:
                    for unit in ['<s>', '<m>', '<m/s>', '<ps>', '<bytes>']:
                        buff = strip_unit(words[1], unit)
                        if buff is None:
                            continue
                        self.mph[words[0]] = float(buff)

    def __get_sph__(self):
        """
        read Sciamachy level 0 SPH header
        """
        def strip_unit(mystr: str, key: str) -> str:
            indx = mystr.find(key)
            if indx != -1:
                return mystr[0:indx]
            return None

        with open(self.filename, 'rt', encoding='latin-1') as fp:
            fp.seek(lv0_consts('mph_size'))     # skip MPH header

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
                elif words[1].find('>') == -1:
                    self.sph[words[0]] = int(words[1])
                else:
                    for unit in ['<10-6degE>', '<10-6degN>',
                                 '<deg>', '<%>', '<>']:
                        buff = strip_unit(words[1], unit)
                        if buff is None:
                            continue
                        self.mph[words[0]] = float(buff)
                        if unit in ('<10-6degE>', '<10-6degN>'):
                            self.mph[words[0]] *= 1e-6

    def __get_dsd__(self):
        """
        read Sciamachy level 0 DSD records
        """
        num_dsd = 0
        self.dsd = [{}]

        with open(self.filename, 'rt', encoding='latin-1') as fp:
            # skip headers MPH & SPH
            fp.seek(lv0_consts('mph_size') + self.mph['SPH_SIZE']
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
                    self.dsd.append({})

    def bytes_remain(self, isp, read_sofar=0):
        """
        Returns number to be read from a ISP record
        """
        size = (isp['fep_hdr']['length'] + self.__mjd_envi().itemsize
                + self.__fep_hdr().itemsize + self.__packet_hdr().itemsize + 1)
        size -= read_sofar

        return size

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
        # and a copy of the remaining bytes of a ISP
        info_dtype = np.dtype([
            ('mjd', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('bcps', '>u2'),
            ('buff', 'O')
        ])

        # store whole ISP data in each info-record
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
                num_bytes = self.bytes_remain(ds_hdr, ds_hdr_dtype.itemsize)
                # check for header-data corruption
                if num_bytes < 0:
                    print("# Info - read {} of {} ISPs".format(
                        ni, dsd['NUM_DSR']))
                    break

                # copy read buffer
                for key in ds_hdr_dtype.names:
                    ds_rec[key] = ds_hdr[key]

                # set quality flag to zero
                ds_rec['fep_hdr']['_quality'] = 0

                ds_rec['data_hdr']['packet_type'] >>= 4
                ds_rec['data_hdr']['packet_type'] &= 0x3
                if ds_rec['fep_hdr']['length'] == 6813:
                    flag_packet = ds_rec['data_hdr']['packet_type'] == 3
                    flag_length = ds_rec['data_hdr']['length'] == 12
                    if flag_packet and flag_length:
                        pass  # this is certainly a PMD ISP
                    elif flag_packet:
                        ds_rec['data_hdr']['length'] = 12
                    elif flag_length:
                        ds_rec['data_hdr']['packet_type'] = 3
                    else:
                        print("# Warning - ISP[{}] unknown packet".format(ni))
                        print("# * feb_hdr: ", ds_rec['fep_hdr'])
                        print("# * packted_hdr: ", ds_rec['packet_hdr'])
                        print("# * data_hdr: ", ds_rec['data_hdr'])
                        ds_rec['data_hdr']['packet_type'] = 0
                        fp.seek(num_bytes, 1)
                        continue

                    num_pmd += 1
                    offs_bcps = 32
                    if last_pmd_icu_time > ds_rec['data_hdr']['icu_time']:
                        self.sorted['pmd'] = False
                    last_pmd_icu_time = ds_rec['data_hdr']['icu_time']
                elif ds_rec['fep_hdr']['length'] == 1659:
                    flag_packet = ds_rec['data_hdr']['packet_type'] == 2
                    flag_length = ds_rec['data_hdr']['length'] == 30
                    if flag_packet and flag_length:
                        pass  # this is certainly a auxiliary ISP
                    elif flag_packet:
                        ds_rec['data_hdr']['length'] = 30
                    elif flag_length:
                        ds_rec['data_hdr']['packet_type'] = 2
                    else:
                        print("# Warning - ISP[{}] unknown packet".format(ni))
                        print("# * feb_hdr: ", ds_rec['fep_hdr'])
                        print("# * packted_hdr: ", ds_rec['packet_hdr'])
                        print("# * data_hdr: ", ds_rec['data_hdr'])
                        ds_rec['data_hdr']['packet_type'] = 0
                        fp.seek(num_bytes, 1)
                        continue

                    num_aux += 1
                    offs_bcps = 20
                    if last_aux_icu_time > ds_rec['data_hdr']['icu_time']:
                        self.sorted['aux'] = False
                    last_aux_icu_time = ds_rec['data_hdr']['icu_time']
                else:
                    flag_packet = ds_rec['data_hdr']['packet_type'] == 1
                    flag_length = ds_rec['data_hdr']['length'] == 66
                    if flag_packet and flag_length:
                        pass  # this is certainly a detector ISP
                    elif flag_packet:
                        ds_rec['data_hdr']['length'] = 66
                    elif flag_length:
                        ds_rec['data_hdr']['packet_type'] = 1
                    else:
                        print("# Warning - ISP[{}] unknown packet".format(ni))
                        print("# * feb_hdr: ", ds_rec['fep_hdr'])
                        print("# * packted_hdr: ", ds_rec['packet_hdr'])
                        print("# * data_hdr: ", ds_rec['data_hdr'])
                        ds_rec['data_hdr']['packet_type'] = 0
                        fp.seek(num_bytes, 1)
                        continue

                    num_det += 1
                    offs_bcps = 0
                    if last_det_icu_time > ds_rec['data_hdr']['icu_time']:
                        self.sorted['det'] = False
                    last_det_icu_time = ds_rec['data_hdr']['icu_time']

                # read remainder of ISP
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

        print('# Info - number of ISPs (sorted={}): {:4d} {:3d} {:3d}'.format(
            self.sorted['det'] & self.sorted['aux'] & self.sorted['pmd'],
            num_det, num_aux, num_pmd))

    # repair SCIAMACHY_SOURCE_PACKETS
    def repair_info(self):
        """
        Repairs chronological order of info records

        Description
        -----------
        Checks attribute 'sorted', nothing is done when ISP's are sorted
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
                        info['data_hdr']['icu_time'][indx] = \
                            info['data_hdr']['icu_time'][indx-1]
                        print("# Info - icu_time of {}_isp[{}] fixed".format(
                            name, indx))
                    else:
                        # print(info['data_hdr']['icu_time'][indx-1:indx+2])
                        # print(info['data_hdr']['state_id'][indx-1:indx+2])
                        # print(info['bcps'][indx-1:indx+2])
                        print("# Warning - (a) can not fix {}_isp[{}]".format(
                            name, indx))
                else:
                    print("# Warning - (b) can not fix {}_isp[{}]".format(
                        name, indx))

            # 2) put state executions in chronological order
            #  and remove repeated states or partly repeated states in product
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
                        print("# Warning - rejected {}_isp: {}".format(
                            name, indx))
                        continue

                    info_list += (info[indx[blocks[num]:blocks[num+1]]],)
            else:
                info_list += (info,)

        # combine all detector, auxiliary and pmd ISP's
        self.info = np.concatenate(info_list)

    def __read_det_raw(self, ds_rec):
        """
        read detector ISP's without any checks
        """
        det = np.empty(1, dtype=self.det_isp_dtype())[0]

        # copy fixed part of the detector ISP
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
        channel['hdr'][:] = 0
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

    def __read_det_safe(self, ds_rec, det_indx):
        """
        read detector ISP's with sanity checks
        """
        det = np.empty(1, dtype=self.det_isp_dtype())[0]

        # copy fixed part of the detector ISP
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
        channel['hdr'][:] = 0
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
            # print(det_indx, nch, ds_rec['fep_hdr']['crc_errs'],
            #      det['pmtc_hdr']['num_chan'],
            #      channel['hdr']['sync'][nch],
            #      channel['hdr']['clusters'][nch],
            #      offs, len(ds_rec['buff']))
            channel['hdr']['clusters'][nch] &= 0xF
            if channel['hdr']['sync'][nch] != 0xAAAA:
                print("# Warning - channel-sync corruption", det_indx, nch)
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
                # print(det_indx, nch, ncl, ds_rec['fep_hdr']['crc_errs'],
                #      det['pmtc_hdr']['num_chan'],
                #      channel['hdr']['sync'][nch],
                #      channel['hdr']['clusters'][nch],
                #      hdr['sync'][ncl], hdr['start'][ncl],
                #      hdr['length'][ncl], hdr['coaddf'][ncl],
                #      offs, len(ds_rec['buff']))
                if hdr['sync'][ncl] != 0xBBBB:
                    print("# Warning - cluster-sync corruption",
                          det_indx, nch, ncl)
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
                              det_indx, nch, ncl)
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
                              det_indx, nch, ncl)
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

    # read SCIAMACHY_SOURCE_PACKETS
    def get_isp(self, state_id=None):
        """
        read Sciamachy level 0 ISP records into numpy compound-arrays

        Parameters
        ----------
        state_id : list
         read only ISP's of selected states

        Returns
        -------
        numpy array

        Description
        -----------
        First all ISPs are read from disk, only the common data-headers of the
        level 0 detector, auxiliary and PMD packets are stored in structured
        numpy arrays. The remainder of the data packets are read as byte arrays
        from disk according to the ISP size specified in the FEP header.

        In a second run, all information stored in the ISPs are stored in
        structured numpy arrays. The auxiliary and PMD ISPs have a fixed size,
        and can be copied using the function numpy.frombuffer(). The detector
        ISPs have to be read dynamically, as their size can vary based on the
        instrument settings (defined by the state ID). The shape and size of
        the detector ISP is defined by the number of channels, the number of
        clusters per channel, the number of pixels per cluster and the
        co-adding factor of the read-outs. All these parameters are stored in
        the data headers of each ISP. Any of these parameters can be
        corrupted, which result in a incorrect interpretation of the ISP.
        Only a very small faction of the data is affected by data corruption,
        reported in the FEP header parameters CRC errors and RS errors. When,
        the interpretation of the data streams fails, we re-read the ISP with
        a much slower fail-safe interpretation, this routine performes various
        sanity checks and will ingnore data of a ISP after a data coruption,
        but will interpret any remaining ISPs.
        """
        if self.info is None:
            self.__get_info__()

        # ----- read level 0 detector data packets -----
        # possible variants: raw, safe, clus_def
        indx_det = np.where(self.info['data_hdr']['packet_type'] == 1)[0]
        if indx_det.size > 0:
            det_isp = np.empty(len(indx_det), dtype=self.det_isp_dtype())

            ni = 0
            for ds_rec in self.info[indx_det]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                try:
                    det_isp[ni] = self.__read_det_raw(ds_rec)
                except (IndexError, ValueError, RuntimeError):
                    det_isp[ni] = self.__read_det_safe(ds_rec, ni)
                ni += 1

            det_isp = det_isp[0:ni]
            print("# Info - read {} detector ISP".format(ni))

        # ----- read level 0 auxiliary data packets -----
        indx_aux = np.where(self.info['data_hdr']['packet_type'] == 2)[0]
        if indx_aux.size > 0:
            aux_isp = np.empty(len(indx_aux), dtype=self.aux_isp_dtype())

            ni = 0
            for ds_rec in self.info[indx_aux]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                # copy fixed part of the auxiliary ISP
                aux = aux_isp[ni]
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

            aux_isp = aux_isp[0:ni]
            print("# Info - read {} auxiliary ISP".format(ni))

        # ----- read level 0 PMD data packets -----
        indx_pmd = np.where(self.info['data_hdr']['packet_type'] == 3)[0]
        if indx_pmd.size > 0:
            pmd_isp = np.empty(len(indx_pmd), dtype=self.pmd_isp_dtype())

            ni = 0
            for ds_rec in self.info[indx_pmd]:
                # print(ds_rec['fep_hdr'], ds_rec['packet_hdr'],
                #      ds_rec['data_hdr'])
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                # copy fixed part of the PMD ISP
                pmd = pmd_isp[ni]
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

            pmd_isp = pmd_isp[0:ni]
            print("# Info - read {} PMD ISP".format(ni))

        return (det_isp, aux_isp, pmd_isp)

    def get_channel(self, state_id, chan_id):
        """
        combines readouts of one science channel for a given state ID
        """
        if not isinstance(state_id, int):
            raise ValueError("state_id must be an integer")

        det_isp, _, _ = self.get_isp([state_id])
        if det_isp.size == 0:
            return None
        chan = np.empty(det_isp.size, dtype=self.chan_dtype())
        chan['time'][:] = mjd_to_datetime(state_id, det_isp)
        chan['data'][...] = np.nan

        for ni, dsr in enumerate(det_isp):
            chan['icu_time'][ni] = dsr['data_hdr']['icu_time']
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
