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


def check_dsr_in_states(mds_in, indx_mds, verbose=False, check=False):
    """
    This module combines L0 DSR per state ID based on parameter icu_time.
    """
    # initialize quality of all DSRs to zero
    mds = mds_in[indx_mds]
    mds['fep_hdr']['_quality'] = 0

    # combine L0 DSR on parameter icu_time
    # alternatively one could use parameter state_id
    _arr = mds['data_hdr']['icu_time']
    _arr = np.concatenate(([-1], _arr, [-1]))
    indx = np.where(np.diff(_arr) != 0)[0]
    num_dsr = np.diff(indx)
    icu_time = mds['data_hdr']['icu_time'][indx[:-1]]
    state_id = mds['data_hdr']['state_id'][indx[:-1]]
    if verbose:
        for ni in range(num_dsr.size):
            print('# ', ni, indx[ni], num_dsr[ni],
                  state_id[ni], icu_time[ni])

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

        print('# rejected {} DSRs'.format(
            np.sum(mds['fep_hdr']['_quality'] != 0)))

    return mds


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
        for magic, _ in magic_dict.items():
            with open(flname, 'rb') as fp:
                file_magic = fp.read(len(magic))

            if file_magic == magic:
                raise SystemError("can not read compressed file")

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

    def ds_info_dtype(self):
        """
        Returns only the common part of (aux, det, pmd) MDS records
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr())
        ])

    def ds_buff_dtype(self):
        """
        Returns ds_info + a python object
        """
        return np.dtype([
            ('isp', self.__mjd_envi()),
            ('fep_hdr', self.__fep_hdr()),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('bcps', '>u2'),
            ('buff', 'O')
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
            ('bcp', aux_bcp_dtype, (lv0_consts('num_lv0_aux_bcp'))),
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
             lv0_consts('num_lv0_aux_pmtc_frame'))
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
        for key in self.ds_info_dtype().names:
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
                                              offset=offs)[0]
                else:
                    nbytes = 3 * hdr['length'][ncl]
                    buff[ncl] = np.frombuffer(ds_rec['buff'],
                                              dtype='u1',
                                              count=nbytes,
                                              offset=offs)[0]
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
        for key in self.ds_info_dtype().names:
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
                print('# channel-sync corruption', ni, nch)
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
                    print('# cluster-sync corruption', ni, nch, ncl)
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
                        print('# cluster-size corruption', ni, nch, ncl)
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
                        print('# cluster-size corruption', ni, nch, ncl)
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
    def get_mds(self, state_id=None):
        """
        read Sciamachy level 0 MDS records
        """
        indx_aux = []
        indx_det = []
        indx_pmd = []

        # select DSD with name 'SCIAMACHY_SOURCE_PACKETS'
        dsd = None
        for dsd in self.dsd:
            if dsd['DS_NAME'] == 'SCIAMACHY_SOURCE_PACKETS':
                break

        # store all MDS data in these buffers
        ds_buffer = np.empty(dsd['NUM_DSR'], dtype=self.ds_buff_dtype())

        # collect information about the level 0 measurement data in product
        with open(self.filename, 'rb') as fp:
            ds_info_dtype = self.ds_info_dtype()
            fp.seek(dsd['DS_OFFSET'])
            for ni, ds_rec in enumerate(ds_buffer):
                ds_info = np.fromfile(fp, dtype=ds_info_dtype, count=1)[0]

                # check for corrupted data
                num_bytes = self.bytes_left(ds_info, ds_info_dtype.itemsize)
                if num_bytes < 0:
                    print('# read {} of {} DSRs'.format(ni, dsd['NUM_DSR']))
                    break

                # copy read buffer
                for key in ds_info_dtype.names:
                    ds_rec[key] = ds_info[key]

                ds_rec['data_hdr']['packet_type'] >>= 4
                if ds_rec['data_hdr']['packet_type'] == 1:
                    indx_det.append(ni)
                    offs_bcps = 0
                elif (ds_rec['data_hdr']['packet_type'] == 2
                      or ds_rec['fep_hdr']['length'] == 1659):
                    indx_aux.append(ni)
                    offs_bcps = 20
                elif (ds_rec['data_hdr']['packet_type'] == 3
                      or ds_rec['fep_hdr']['length'] == 6813):
                    indx_pmd.append(ni)
                    offs_bcps = 32
                else:
                    print('# warning: unknown packet type {}'.format(
                        ds_rec['data_hdr']['packet_type']))
                    ds_rec['data_hdr']['packet_type'] = 1
                    indx_det.append(ni)
                    offs_bcps = 0

                # read remainder of DSR
                ds_rec['buff'] = fp.read(num_bytes)

                # read BCPS
                ds_rec['bcps'] = np.frombuffer(
                    ds_rec['buff'], '>i2', count=1, offset=offs_bcps)

        print('# number of DSRs: ',
              len(indx_det), len(indx_aux), len(indx_pmd))

        # ----- read level 0 detector data packets -----
        # possible variants: raw, safe, clus_def 
        if indx_det:
            det_mds = np.empty(len(indx_det), dtype=self.det_mds_dtype())

            ni = 0
            for ds_rec in ds_buffer[indx_det]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                try:
                    det_mds[ni] = self.__read_det_raw(ds_rec, ni, det_mds)
                except (ValueError, RuntimeError):
                    det_mds[ni] = self.__read_det_safe(ds_rec, ni, det_mds)
                ni += 1

        print('# read {} detector mds'.format(ni))

        # ----- read level 0 auxiliary data packets -----
        if indx_aux:
            aux_mds = np.empty(len(indx_aux), dtype=self.aux_mds_dtype())

            ni = 0
            for ds_rec in ds_buffer[indx_aux]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                # copy fixed part of the auxiliary MDS
                aux = aux_mds[ni]
                for key in ds_info_dtype.names:
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
                    count=lv0_consts('num_lv0_aux_pmtc_frame'),
                    offset=self.__aux_pmtc_hdr().itemsize)
                ni += 1

        print('# read {} auxiliary mds'.format(ni))

        # ----- read level 0 PMD data packets -----
        if indx_pmd:
            pmd_mds = np.empty(len(indx_pmd), dtype=self.pmd_mds_dtype())

            ni = 0
            for ds_rec in ds_buffer[indx_pmd]:
                if state_id is not None:
                    if ds_rec['data_hdr']['state_id'] not in state_id:
                        continue

                # copy fixed part of the PMD MDS
                pmd = pmd_mds[ni]
                for key in ds_info_dtype.names:
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
                    count=lv0_consts('num_lv0_pmd_packets'),
                    offset=2)
                ni += 1

        print('# read {} PMD mds'.format(ni))

        return (det_mds, aux_mds, pmd_mds)
