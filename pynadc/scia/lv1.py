"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to read Sciamachy level 1b data products

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

    if key is None:
        return consts
    if key in consts:
        return consts[key]

    raise KeyError('level 0 constant {} is not defined'.format(key))


# - Classes --------------------------------------
class File:
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
            ('mjd', {'names': ['days', 'secnds', 'musec'],
                     'formats': ['>i4', '>u4', '>u4']}),
            ('flag_attached', 'i1'),
            ('mean_wv_diff', '>f4', (8)),
            ('sdev_wv_diff', '>f4', (8)),
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
            ('mjd', {'names': ['days', 'secnds', 'musec'],
                     'formats': ['>i4', '>u4', '>u4']}),
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
            ('do_var_lc_cha', '4a', (3)),
            ('do_stray_lc_cha', '4a', (8)),
            ('do_var_lc_pmd', '4a', (2)),
            ('do_stray_lc_pmd', '4a', (7)),
            ('electron_bu', '>f4', (8)),
            ('ppg_error', '>f4'),
            ('stray_error', '>f4'),
            ('sp_n_phases', 'u1'),
            ('sp_phase_boundaries', '>f4', (13)),
            ('startpix_6', '>u2'),
            ('startpix_8', '>u2'),
            ('h_toa', '>f4'),
            ('lambda_end_gdf', '>f4'),
            ('do_pol_point', 'c', (12)),
            ('sat_level', '>u2', (8)),
            ('pmd_saturation_limit', '>u2'),
            ('do_use_limb_dark', 'c'),
            ('do_pixelwise', 'c', (8)),
            ('alpha0_asm', '>f4'),
            ('alpha0_ems', '>f4'),
            ('do_fraunhofer', '5a', (8)),
            ('do_etalon', '3a', (8)),
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
            ('fpn', '>f4', (8192)),
            ('fpn_error', '>f4', (8192)),
            ('lc', '>f4', (8192)),
            ('lc_error', '>f4', (8192)),
            ('pmd_dark', '>f4', (14)),
            ('pmd_dark_error', '>f4', (14)),
            ('mean_noise', '>f4', (8192))
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
            ('stray', '>f4', (8192)),
            ('stray_error', '>f4', (8192)),
            ('pmd_stray', '>f4', (7)),
            ('pmd_stray_error', '>f4', (7)),
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
            ('ppg_fact', '>f4', (8192)),
            ('etalon_fact', '>f4', (8192)),
            ('etalon_resid', '>f4', (8192)),
            ('wls_deg_fact', '>f4', (8192)),
            ('bdpm', 'u1', (8192))
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
            ('wavelen_grid', '>f4', (8192))
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
            ('coeffs', '>f8', (8, 5)),
            ('num_lines', '>u2', (8)),
            ('wavelen_error', '>f4', (8)),
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
            ('wavelength', '>f4', (8192)),
            ('smr', '>f4', (8192)),
            ('smr_precision', '>f4', (8192)),
            ('smr_accuracy', '>f4', (8192)),
            ('etalon', '>f4', (8192)),
            ('avg_azi', '>f4'),
            ('avg_ele', '>f4'),
            ('avg_sun_ele', '>f4'),
            ('mean_pmd', '>f4', (7)),
            ('pmd_nd_out', '>f4', (7)),
            ('pmd_nd_in', '>f4', (7)),
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
            ('mu2', '>f4', (8192)),
            ('mu3', '>f4', (8192))
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
            ('mu2', '>f4', (8192)),
            ('mu3', '>f4', (8192))
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
            ('mu2', '>f4', (8192)),
            ('mu3', '>f4', (8192))
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
            ('sensitivity', '>f4', (8192))
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
            ('sensitivity', '>f4', (8192))
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
            ('sensitivity', '>f4', (8192))
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
            ('mu2_nadir', '>f4', (8192)),
            ('mu3_nadir', '>f4', (8192)),
            ('mu2_limb', '>f4', (8192)),
            ('mu3_limb', '>f4', (8192)),
            ('sensitivity_obm', '>f4', (8192)),
            ('sensitivity_nadir', '>f4', (8192)),
            ('sensitivity_limb', '>f4', (8192)),
            ('sensitivity_sun', '>f4', (8192)),
            ('bsdf', '>f4', (8192))
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

    def get_states(self):
        """
        read State definitions of the product
        """
        record_dtype = np.dtype([
            ('mjd', {'names': ['days', 'secnds', 'musec'],
                     'formats': ['>i4', '>u4', '>u4']}),
            ('flag_attached', 'i1'),
            ('flag_reason', 'i1'),
            ('orbit_phase', '>f4'),
            ('category', '>u2'),
            ('state_id', '>u2'),
            ('duration', '>u2'),
            ('intg_max', '>u2'),
            ('num_clus', '>u2'),
            ('Clcon', {'names': ['clus_id', 'chan_id', 'start', 'length',
                                 'pet', 'intg', 'coaddf', 'readouts',
                                 'clus_type'],
                       'formats': ['u1', 'u1', '>u2', '>u2', '>f4',
                                   '>u2', '>u2', '>u2', 'u1']}, (64)),
            ('mds_type', 'u1'),
            ('num_geo', '>u2'),
            ('num_pmd', '>u2'),
            ('num_intg', '>u2'),
            ('intg', '>u2', (64)),
            ('polv', '>u2', (64)),
            ('num_polv', '>u2'),
            ('number', '>u2'),
            ('length', '>u4')
        ])
        dsd = self.dsd_by_name('STATES')
        with open(self.filename, 'rb') as fp:
            fp.seek(dsd['DS_OFFSET'])
            buff = np.fromfile(fp, dtype=record_dtype, count=dsd['NUM_DSR'])
        return buff
