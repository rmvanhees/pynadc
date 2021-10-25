"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to read Sciamachy level 1b data products (ESA/PDS format)

The SRON Sciamachy level 1b data set contains 48442 products from the period:
 18 June 2002 until 08 April 2012

Statistics on proc-stage
    166 P
     22 R
    428 W
  47826 Y

Copyright (c) 2012-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from datetime import timedelta
from pathlib import Path

import numpy as np

# - global parameters ------------------------------


# - local functions --------------------------------
def lv1_consts(key=None):
    """
    defines consts used while reading Sciamachy level 0 data
    """
    consts = {}
    consts['mds_size'] = 1247

    consts['max_clusters'] = 64
    consts['uvn_channels'] = 5
    consts['swir_channels'] = 3
    consts['all_channels'] = consts['uvn_channels'] + consts['swir_channels']
    consts['channel_pixels'] = 1024
    consts['all_pixels'] = consts['all_channels'] * consts['channel_pixels']

    consts['num_pmd'] = 7
    consts['num_frac_polv'] = 12
    consts['num_spec_coeffs'] = 5

    if key is None:
        return consts
    if key in consts:
        return consts[key]

    raise KeyError('level 1b constant {} is not defined'.format(key))


def scale_mem_nlin(chan_id, rvals):
    """
    scale memory/non-linearity values
    """
    if chan_id < 6:
        return 1.25 * (rvals + 37)

    if chan_id == 6:
        return 1.25 * (rvals + 102)

    if chan_id == 7:
        return 1.5 * (rvals + 102)

    if chan_id == 8:
        return 1.25 * (rvals + 126)

    raise ValueError("invalid channel ID: {}".format(chan_id))


# - Classes --------------------------------------
class File:
    """
    Class to read Sciamachy level 1b products (ESA/PDS format)
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
        if 'SPH_DESCRIPTOR' not in self.sph:
            raise ValueError('SPH_DESCRIPTOR not found in product header')
        if not self.sph['SPH_DESCRIPTOR'].startswith("SCI_NL__1P SPECIFIC"):
            raise ValueError('not a Sciamachy level 1B product')

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

    @staticmethod
    def __pmtc_hdr():
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

    def __lv0_hdr(self):
        """
        Returns numpy-dtype definition for a generic level 0 DSR header
        """
        return np.dtype([
            ('bcps', '>u2'),
            ('num_chan', '>u2'),
            ('orbit_vector', '>i4', (8)),
            ('packet_hdr', self.__packet_hdr()),
            ('data_hdr', self.__data_hdr()),
            ('pmtc_hdr', self.__pmtc_hdr())
        ])

    @staticmethod
    def __coord():
        return np.dtype([
            ('lat', '>i4'),
            ('lon', '>i4')
        ])

    def __geo_limb(self):
        """
        Returns numpy-dtype definition for geolocation limb measurement
        """
        return np.dtype([
            ('esm_pos', '>f4'),
            ('asm_pos', '>f4'),
            ('solar_zenith', '>f4', (3)),
            ('solar_azimuth', '>f4', (3)),
            ('los_zenith', '>f4', (3)),
            ('los_azimuth', '>f4', (3)),
            ('altitude', '>f4'),
            ('earth_radius', '>f4'),
            ('sub_sat_point', self.__coord()),
            ('tangent_point', self.__coord(), (3)),
            ('tangent_height', '>f4', (3)),
            ('doppler_shift', '>f4')
        ])

    def __geo_nadir(self):
        """
        Returns numpy-dtype definition for geolocation nadir measurement
        """
        return np.dtype([
            ('esm_pos', '>f4'),
            ('solar_zenith', '>f4', (3)),
            ('solar_azimuth', '>f4', (3)),
            ('los_zenith', '>f4', (3)),
            ('los_azimuth', '>f4', (3)),
            ('altitude', '>f4'),
            ('earth_radius', '>f4'),
            ('sub_sat_point', self.__coord()),
            ('corners', self.__coord(), (4)),
            ('center', self.__coord())
        ])

    def __geo_mon(self):
        """
        Returns numpy-dtype definition for geolocation monitor measurement
        """
        return np.dtype([
            ('esm_pos', '>f4'),
            ('asm_pos', '>f4'),
            ('solar_zenith', '>f4', (3)),
            ('sub_sat_point', self.__coord())
        ])

    @staticmethod
    def __frac_pol():
        """
        Returns numpy-dtype definition for fractional polarisation values
        """
        return np.dtype([
            ('q_val', '>f4', (12)),
            ('q_err', '>f4', (12)),
            ('u_val', '>f4', (12)),
            ('u_err', '>f4', (12)),
            ('wv', '>f4', (13)),
            ('gdf', '>f4', (3))
        ])

    @staticmethod
    def __lv1_clus(coaddf):
        """
        Returns numpy-dtype definition for level 1b cluster data
        """
        if coaddf == 1:
            return np.dtype([
                ('mem', 'i1'),
                ('sign', '>u2'),
                ('stray', 'u1')
            ])
        # struct is hard to read!
        # sign contains
        #     coadded detector signal (unsigned 24bit: sign & 0xffffff)
        #     correction (signed 8 bit) (sign >> 24) > 127: val - 256
        return np.dtype([
            ('sign', '>u4'),
            ('stray', 'u1')
        ])

    def mds_dtype(self, state):
        """
        Returns numpy-dtype definition for a level 1b mds record
        """
        n_aux = state['num_geo'] // state['num_dsr']
        n_pmd = lv1_consts('num_pmd') * state['num_pmd'] // state['num_dsr']
        n_polv = state['num_polv'] // state['num_dsr']

        if state['mds_type'] == 1:   # Nadir
            dtype_list = [
                ('mjd', self.__mjd_envi()),
                ('dsr_length', '>u4'),
                ('quality_flag', 'u1'),
                ('scale_factor', 'u1', (lv1_consts('all_channels'))),
                ('sat_flag', 'u1', (n_aux)),
                ('red_grass', 'u1', (n_aux, state['num_clus'])),
                ('sun_glint', 'u1', (n_aux)),
                ('geo', self.__geo_nadir(), (n_aux)),
                ('lv0_hdr', self.__lv0_hdr(), (n_aux)),
                ('pmd', '>f4', (n_pmd)),
                ('frac_pol', self.__frac_pol(), (n_polv))
            ]
        elif state['mds_type'] in [2, 3]:   # Limb & Occultation
            dtype_list = [
                ('mjd', self.__mjd_envi()),
                ('dsr_length', '>u4'),
                ('quality_flag', 'u1'),
                ('scale_factor', 'u1', (lv1_consts('all_channels'))),
                ('sat_flag', 'u1', (n_aux)),
                ('red_grass', 'u1', (n_aux, state['num_clus'])),
                ('sun_glint', 'u1', (n_aux)),
                ('geo', self.__geo_limb(), (n_aux)),
                ('lv0_hdr', self.__lv0_hdr(), (n_aux)),
                ('pmd', '>f4', (n_pmd)),
                ('frac_pol', self.__frac_pol(), (n_polv))
            ]
        else:
            dtype_list = [
                ('mjd', self.__mjd_envi()),
                ('dsr_length', '>u4'),
                ('quality_flag', 'u1'),
                ('scale_factor', 'u1', (lv1_consts('all_channels'))),
                ('sat_flag', 'u1', (n_aux)),
                ('red_grass', 'u1', (n_aux, state['num_clus'])),
                ('sun_glint', 'u1', (n_aux)),
                ('geo', self.__geo_mon(), (n_aux)),
                ('lv0_hdr', self.__lv0_hdr(), (n_aux))
            ]

        for ncl in range(state['num_clus']):
            dims = (state['Clcon']['n_read'][ncl],
                    state['Clcon']['length'][ncl])
            dtype_list.append(
                ('clus_{:02d}'.format(ncl),
                 self.__lv1_clus(state['Clcon']['coaddf'][ncl]),
                 (dims)))

        return np.dtype(dtype_list)

    @staticmethod
    def chan_dtype():
        """
        Returns numpy-dtype definition for science channel data
        """
        return np.dtype([
            ('time', 'datetime64[us]'),
            ('data', 'f8', (lv1_consts('channel_pixels')))
        ])

    # ----- read routines -------------------------
    def __get_mph__(self):
        """
        read Sciamachy level 1b MPH header
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
        read Sciamachy level 1b SPH header
        """
        def strip_unit(mystr: str, key: str) -> str:
            indx = mystr.find(key)
            if indx != -1:
                return mystr[0:indx]
            return None

        with open(self.filename, 'rt', encoding='latin-1') as fp:
            fp.seek(lv1_consts('mds_size'))     # skip MPH header

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
        read Sciamachy level 1b DSD records
        """
        num_dsd = 0
        self.dsd = [{}]

        with open(self.filename, 'rt', encoding='latin-1') as fp:
            # skip headers MPH & SPH
            fp.seek(lv1_consts('mds_size') + self.mph['SPH_SIZE']
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

    def dsd_by_name(self, ds_name):
        """
        Returns DSD record with dsd['DS_NAME'] equals ds_name
        """
        dsd = None
        for dsd in self.dsd:
            if dsd['DS_NAME'] == ds_name:
                break

        return dsd

    def get_sqads(self):
        """
        read Summary of Quality Flags per State (SQADS)
        """
        record_dtype = np.dtype([
            ('mjd', self.__mjd_envi()),
            ('flag_attached', 'i1'),
            ('mean_wv_diff', '>f4', (lv1_consts('all_channels'))),
            ('sdev_wv_diff', '>f4', (lv1_consts('all_channels'))),
            ('spare1', '>u2'),
            ('mean_lc_diff', '>f4', (15)),
            ('flag_sunglint', 'u1'),
            ('flag_rainbow', 'u1'),
            ('flag_saa', 'u1'),
            ('num_hot', '>u2', (15)),
            ('spare', '>u1', (10))
        ])
        dsd = self.dsd_by_name('SUMMARY_QUALITY')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_lads(self):
        """
        read Geolocation of the States (LADS)
        """
        record_dtype = np.dtype([
            ('mjd', self.__mjd_envi()),
            ('flag_attached', 'i1'),
            ('corners', {'names': ['lat', 'lon'],
                         'formats': ['>i4', '>i4']}, (4))
        ])
        dsd = self.dsd_by_name('GEOLOCATION')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_sip(self):
        """
        read Static Instrument Parameters (SIP)
        """
        record_dtype = np.dtype([
            ('n_lc_min', 'u1'),
            ('ds_n_phase', 'u1'),
            ('ds_phase_boundaries', '>f4', (13)),
            ('lc_stray_index', '>f4', (2)),
            ('lc_harm_order', 'u1'),
            ('ds_poly_order', 'u1'),
            ('do_var_lc_cha', '4a', (lv1_consts('swir_channels'))),
            ('do_stray_lc_cha', '4a', (lv1_consts('all_channels'))),
            ('do_var_lc_pmd', '4a', (2)),
            ('do_stray_lc_pmd', '4a', (lv1_consts('num_pmd'))),
            ('electron_bu', '>f4', (lv1_consts('all_channels'))),
            ('ppg_error', '>f4'),
            ('stray_error', '>f4'),
            ('sp_n_phases', 'u1'),
            ('sp_phase_boundaries', '>f4', (13)),
            ('startpix_6', '>u2'),
            ('startpix_8', '>u2'),
            ('h_toa', '>f4'),
            ('lambda_end_gdf', '>f4'),
            ('do_pol_point', 'c', (12)),
            ('sat_level', '>u2', (lv1_consts('all_channels'))),
            ('pmd_saturation_limit', '>u2'),
            ('do_use_limb_dark', 'c'),
            ('do_pixelwise', 'c', (lv1_consts('all_channels'))),
            ('alpha0_asm', '>f4'),
            ('alpha0_ems', '>f4'),
            ('do_fraunhofer', '5a', (lv1_consts('all_channels'))),
            ('do_etalon', '3a', (lv1_consts('all_channels'))),
            ('do_IB_SD_ETN', 'c', (7)),
            ('do_IB_OC_ETN', 'c', (7)),
            ('level_2_SMR', 'u1', (8))
        ])
        dsd = self.dsd_by_name('INSTRUMENT_PARAMS')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_clcp(self):
        """
        read Leakage Current Parameters (CLCP)
        """
        record_dtype = np.dtype([
            ('fpn', '>f4', (lv1_consts('all_pixels'))),
            ('fpn_error', '>f4', (lv1_consts('all_pixels'))),
            ('lc', '>f4', (lv1_consts('all_pixels'))),
            ('lc_error', '>f4', (lv1_consts('all_pixels'))),
            ('pmd_dark', '>f4', (14)),
            ('pmd_dark_error', '>f4', (14)),
            ('mean_noise', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('LEAKAGE_CONSTANT')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_vlcp(self):
        """
        read Leakage Current Parameters (VLCP)
        """
        record_dtype = np.dtype([
            ('orbit_phase', '>f4'),
            ('temperatures', '>f4', (10)),
            ('var_lc', '>f4', (3072)),
            ('var_lc_error', '>f4', (3072)),
            ('stray', '>f4', (lv1_consts('all_pixels'))),
            ('stray_error', '>f4', (lv1_consts('all_pixels'))),
            ('pmd_stray', '>f4', (lv1_consts('num_pmd'))),
            ('pmd_stray_error', '>f4', (lv1_consts('num_pmd'))),
            ('pmd_var_lc', '>f4', (2)),
            ('pmd_var_lc_error', '>f4', (2))
        ])
        dsd = self.dsd_by_name('LEAKAGE_VARIABLE')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_ppg(self):
        """
        read PPG/Etalon Parameters (PPG)
        """
        record_dtype = np.dtype([
            ('ppg_fact', '>f4', (lv1_consts('all_pixels'))),
            ('etalon_fact', '>f4', (lv1_consts('all_pixels'))),
            ('etalon_resid', '>f4', (lv1_consts('all_pixels'))),
            ('wls_deg_fact', '>f4', (lv1_consts('all_pixels'))),
            ('bdpm', 'u1', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('PPG_ETALON')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_base(self):
        """
        read Precise Basis for Spectral Calibration Parameters (BASE)
        """
        record_dtype = np.dtype([
            ('wavelen_grid', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('SPECTRAL_BASE')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_scp(self):
        """
        read Spectral Calibration Parameters (SCP)
        """
        record_dtype = np.dtype([
            ('orbit_phase', '>f4'),
            ('coeffs', '>f8', (lv1_consts('all_channels'),
                               lv1_consts('num_spec_coeffs'))),
            ('num_lines', '>u2', (lv1_consts('all_channels'))),
            ('wavelen_error', '>f4', (lv1_consts('all_channels'))),
        ])
        dsd = self.dsd_by_name('SPECTRAL_CALIBRATION')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_srs(self):
        """
        read Sun Reference Spectrum (SRS)
        """
        record_dtype = np.dtype([
            ('spec_id', 'a2'),
            ('wavelength', '>f4', (lv1_consts('all_pixels'))),
            ('smr', '>f4', (lv1_consts('all_pixels'))),
            ('smr_precision', '>f4', (lv1_consts('all_pixels'))),
            ('smr_accuracy', '>f4', (lv1_consts('all_pixels'))),
            ('etalon', '>f4', (lv1_consts('all_pixels'))),
            ('avg_azi', '>f4'),
            ('avg_ele', '>f4'),
            ('avg_sun_ele', '>f4'),
            ('mean_pmd', '>f4', (lv1_consts('num_pmd'))),
            ('pmd_nd_out', '>f4', (lv1_consts('num_pmd'))),
            ('pmd_nd_in', '>f4', (lv1_consts('num_pmd'))),
            ('doppler', '>f4')
        ])
        dsd = self.dsd_by_name('SUN_REFERENCE')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_pspn(self):
        """
        read Polarisation Sensitivity Parameters Nadir (PSPN)
        """
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('mu2', '>f4', (lv1_consts('all_pixels'))),
            ('mu3', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('POL_SENS_NADIR')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_pspl(self):
        """
        read Polarisation Sensitivity Parameters Limb (PSPL)
        """
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('mu2', '>f4', (lv1_consts('all_pixels'))),
            ('mu3', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('POL_SENS_LIMB')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_pspo(self):
        """
        read Polarisation Sensitivity Parameters Occultation (PSPO)
        """
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('mu2', '>f4', (lv1_consts('all_pixels'))),
            ('mu3', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('POL_SENS_OCC')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_rspn(self):
        """
        read Radiance Sensitivity Parameters Nadir (RSPN)
        """
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('sensitivity', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('RAD_SEND_NADIR')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_rspl(self):
        """
        read Radiance Sensitivity Parameters Limb (RSPL)
        """
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('sensitivity', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('RAD_SENS_LIMB')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_rspo(self):
        """
        read Radiance Sensitivity Parameters Occultation (RSPO)
        """
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('sensitivity', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('RAD_SENS_OCC')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_ekd(self):
        """
        read Errors on Key Data (EKD)
        """
        record_dtype = np.dtype([
            ('mu2_nadir', '>f4', (lv1_consts('all_pixels'))),
            ('mu3_nadir', '>f4', (lv1_consts('all_pixels'))),
            ('mu2_limb', '>f4', (lv1_consts('all_pixels'))),
            ('mu3_limb', '>f4', (lv1_consts('all_pixels'))),
            ('sensitivity_obm', '>f4', (lv1_consts('all_pixels'))),
            ('sensitivity_nadir', '>f4', (lv1_consts('all_pixels'))),
            ('sensitivity_limb', '>f4', (lv1_consts('all_pixels'))),
            ('sensitivity_sun', '>f4', (lv1_consts('all_pixels'))),
            ('bsdf', '>f4', (lv1_consts('all_pixels')))
        ])
        dsd = self.dsd_by_name('ERRORS_ON_KEY_DATA')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_sfp(self):
        """
        read Slit Function Parameters (SFP)
        """
        record_dtype = np.dtype([
            ('pixel_slit', '>u2'),
            ('type_slit', 'u1'),
            ('fwhm_slit', '>f4'),
            ('fwhm_lorenz', '>f4'),
        ])
        dsd = self.dsd_by_name('SLIT_FUNCTION')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_asfp(self):
        """
        read Small Aperture Function Parameters (ASFP)
        """
        record_dtype = np.dtype([
            ('pixel_slit', '>u2'),
            ('type_slit', 'u1'),
            ('fwhm_slit', '>f4'),
            ('fwhm_lorenz', '>f4'),
        ])
        dsd = self.dsd_by_name('SMALL_AP_SLIT_FUNCTION')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff

    def get_states(self, state_id=None):
        """
        read State definitions of the product
        """
        record_dtype = np.dtype([
            ('mjd', self.__mjd_envi()),
            ('flag_attached', 'i1'),
            ('flag_reason', 'i1'),
            ('orbit_phase', '>f4'),
            ('category', '>u2'),
            ('state_id', '>u2'),
            ('duration', '>u2'),
            ('intg_max', '>u2'),
            ('num_clus', '>u2'),
            ('Clcon', {'names': ['id', 'channel', 'start', 'length', 'pet',
                                 'intg', 'coaddf', 'n_read', 'type'],
                       'formats': ['u1', 'u1', '>u2', '>u2', '>f4',
                                   '>u2', '>u2', '>u2', 'u1']},
             (lv1_consts('max_clusters'))),
            ('mds_type', 'u1'),
            ('num_geo', '>u2'),
            ('num_pmd', '>u2'),
            ('num_intg', '>u2'),
            ('intg', '>u2', (lv1_consts('max_clusters'))),
            ('polv', '>u2', (lv1_consts('max_clusters'))),
            ('num_polv', '>u2'),
            ('num_dsr', '>u2'),
            ('length_dsr', '>u4')
        ])
        dsd = self.dsd_by_name('STATES')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            states = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])

        if state_id is None:
            return states

        if not isinstance(state_id, int):
            raise ValueError("state_id must be an integer")

        indx = np.where((states['flag_attached'] == 0)
                        & (states['state_id'] == state_id))[0]
        return states[indx]

    # read SCIAMACHY_SOURCE_PACKETS
    def get_mds(self, state_id=None):
        """
        read Sciamachy level 1b MDS records into numpy compound-arrays

        state_id : list
         read only DSRs of selected states
        """
        dsd = self.dsd_by_name('NADIR')
        nadir_offs = dsd['DS_OFFSET']
        dsd = self.dsd_by_name('LIMB')
        limb_offs = dsd['DS_OFFSET']
        dsd = self.dsd_by_name('OCCULTATION')
        occul_offs = dsd['DS_OFFSET']
        dsd = self.dsd_by_name('MONITORING')
        moni_offs = dsd['DS_OFFSET']

        # read state definitions
        states = self.get_states()

        # read measurement data sets
        all_mds = []
        with open(self.filename, 'rb') as fp:
            for state in states:
                read_flag = False
                mds_dtype = None
                if state['flag_attached'] != 0:
                    continue

                if state_id is None or state['state_id'] in state_id:
                    read_flag = True
                    mds_dtype = self.mds_dtype(state)
                    # print(state['mds_type'], state['state_id'],
                    #      state['orbit_phase'],
                    #      state['num_clus'], state['num_geo'],
                    #      state['num_dsr'], state['length_dsr'])

                mds = None
                if state['mds_type'] == 1:
                    fp.seek(nadir_offs)
                    nadir_offs += state['num_dsr'] * state['length_dsr']
                elif state['mds_type'] == 2:
                    fp.seek(limb_offs)
                    limb_offs += state['num_dsr'] * state['length_dsr']
                elif state['mds_type'] == 3:
                    fp.seek(occul_offs)
                    occul_offs += state['num_dsr'] * state['length_dsr']
                else:
                    fp.seek(moni_offs)
                    moni_offs += state['num_dsr'] * state['length_dsr']

                if not read_flag:
                    continue

                mds = np.fromfile(fp, mds_dtype, count=state['num_dsr'])

                # check if we read all bytes
                if mds_dtype.itemsize != state['length_dsr']:
                    print('# warning: incomplete read',
                          mds_dtype.itemsize, state['length_dsr'])

                # add all MDS of a state to output tuple
                all_mds.append(mds)

        return all_mds

    def get_channel(self, state_id, chan_id, mem_corr=False, stray_corr=False):
        """
        combines readouts of one science channel for a given state execution
        """
        if not isinstance(state_id, int):
            raise ValueError("state_id must be an integer")

        # read data of all state executions
        all_mds = self.get_mds([state_id])
        if not all_mds:
            return None

        # read instrument settings of these state executions
        states = self.get_states(state_id)

        # loop over each state execution (using index 'ni')
        chan_list = []
        for ni, mds in enumerate(all_mds):
            num_clus = states['num_clus'][ni]
            channel = states['Clcon']['channel'][ni, :]
            start = states['Clcon']['start'][ni, :]
            length = states['Clcon']['length'][ni, :]
            coaddf = states['Clcon']['coaddf'][ni, :]
            n_read = states['Clcon']['n_read'][ni, :]
            intg_mn = states['intg'][ni, states['num_intg'][ni]-1] / 16

            # allocate memory for measurement data (and initialize data to NaN)
            chan = np.empty((mds.size, n_read.max()), dtype=self.chan_dtype())
            chan['data'][...] = np.nan

            for nj, dsr in enumerate(mds):
                mst_time = np.datetime64('2000')
                mst_time += np.timedelta64(timedelta(int(dsr['mjd']['days']),
                                                     int(dsr['mjd']['secnds']),
                                                     int(dsr['mjd']['musec'])))
                chan['time'][nj, :] = (
                    mst_time + int(1000000 * intg_mn) * np.arange(n_read.max()))

                for _nc in range(num_clus):
                    if channel[_nc] != chan_id:
                        continue
                    name = 'clus_{:02d}'.format(_nc)
                    if coaddf[_nc] == 1:
                        sign = dsr[name]['sign']
                    else:
                        sign = dsr[name]['sign'] & 0xffffff

                    # apply memory or non-linearity correction
                    if mem_corr:
                        if coaddf[_nc] == 1:
                            mem = dsr[name]['mem'].astype('f8')
                        else:
                            mem = (dsr[name]['sign'] >> 24).astype('f8')
                            mem[mem > 127] -= 256
                        corr = scale_mem_nlin(chan_id, mem)
                        sign = sign.astype('f8') - coaddf[_nc] * corr

                    # apply stray-light correction
                    if stray_corr:
                        scale = dsr['scale_factor'][chan_id-1] / 10
                        if not mem_corr:
                            sign = sign.astype('f8')
                        sign -= dsr[name]['stray'].astype('f8') / scale

                    step = n_read.max() // n_read[_nc]
                    pslice = np.s_[start[_nc]:start[_nc]+length[_nc]]
                    chan['data'][nj, step-1::step, pslice] = sign
                    # print(ni, nj, _nc, channel[_nc], start[_nc], length[_nc],
                    #      step, pslice, sign.shape)

            chan_list.append(chan)

        return chan_list
