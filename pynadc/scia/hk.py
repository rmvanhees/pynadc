"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Routines to convert Sciamachy house-keeping data from raw counts
to physical units.

Copyright (c) 2018 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from datetime import timedelta

import numpy as np

from bitstring import BitArray


def get_det_temp(channel, raw_tm):
    """
    convert raw temperature counts to Kelvin
    """
    nch = channel - 1
    if nch < 0 or nch > 7:
        raise ValueError('channel must be between 1 and 8')

    tab_tm = [
        (0, 17876, 18312, 18741, 19161, 19574, 19980, 20379,
         20771, 21157, 21908, 22636, 24684, 26550, 28259, 65535),
        (0, 18018, 18456, 18886, 19309, 19724, 20131, 20532,
         20926, 21313, 22068, 22798, 24852, 26724, 28436, 65535),
        (0, 20601, 20996, 21384, 21765, 22140, 22509, 22872,
         23229, 23581, 23927, 24932, 26201, 27396, 28523, 65535),
        (0, 20333, 20725, 21110, 21490, 21863, 22230, 22591,
         22946, 23295, 23640, 24640, 25905, 27097, 28222, 65535),
        (0, 20548, 20942, 21330, 21711, 22086, 22454, 22817,
         23174, 23525, 23871, 24875, 26144, 27339, 28466, 65535),
        (0, 17893, 18329, 18758, 19179, 19593, 20000, 20399,
         20792, 21178, 21931, 22659, 24709, 26578, 28289, 65535),
        (0, 12994, 13526, 14046, 14555, 15054, 15543, 16022,
         16492, 17850, 20352, 22609, 24656, 26523, 28232, 65535),
        (0, 13129, 13664, 14188, 14702, 15204, 15697, 16180,
         16653, 18019, 20536, 22804, 24860, 26733, 28447, 65535)
    ]    # shape (8, 16)

    tab_temp = [
        (179., 180., 185., 190., 195., 200., 205., 210.,
         215., 220., 230., 240., 270., 300., 330., 331.),
        (179., 180., 185., 190., 195., 200., 205., 210.,
         215., 220., 230., 240., 270., 300., 330., 331.),
        (209., 210., 215., 220., 225., 230., 235., 240.,
         245., 250., 255., 270., 290., 310., 330., 331.),
        (209., 210., 215., 220., 225., 230., 235., 240.,
         245., 250., 255., 270., 290., 310., 330., 331.),
        (209., 210., 215., 220., 225., 230., 235., 240.,
         245., 250., 255., 270., 290., 310., 330., 331.),
        (179., 180., 185., 190., 195., 200., 205., 210.,
         215., 220., 230., 240., 270., 300., 330., 331.),
        (129., 130., 135., 140., 145., 150., 155., 160.,
         165., 180., 210., 240., 270., 300., 330., 331.),
        (129., 130., 135., 140., 145., 150., 155., 160.,
         165., 180., 210., 240., 270., 300., 330., 331.)
    ]    # shape (8, 16)

    # use linear interpolation (nothing fancy)
    return np.interp(raw_tm, tab_tm[nch], tab_temp[nch])


def get_det_vis_pet(chan_hdr):
    """
    convert raw timing data to detector data to pixel-exposure-time (VIS)
    """
    # The layout of the detector command word for channels 1--5
    #  14 bits: exposure time factor (ETF)
    #       ETF >= 1: pet = etf * 62.5 ms * ratio
    #       ETF == 0: pet = 31.25 ms
    #   2 bits: mode
    #       0: Normal Mode
    #       1: Normal Mode
    #       2: Test Mode
    #       3: ADC calibration
    #   9 bits: section address (2 pixels resolution)
    #       start of virtual channel b at 2 * section
    #   5 bits: ratio
    #       ratio of exposure time between virtual channels
    #   2 bits: control
    #       1: restart of readout cycle
    #       3: hardware reset of detector module electronics
    #
    command = BitArray(uintbe=chan_hdr['command'], length=32)
    etf = int(command.bin[0:14], 2)
    section = int(command.bin[16:25], 2)
    ratio = int(command.bin[25:30], 2)

    vir_chan_b = 2 * section
    if etf == 0:
        return (1 / 32, vir_chan_b)

    pet = etf / 16
    if section > 0 and ratio > 1:
        return ([pet * ratio, pet], vir_chan_b)

    return (pet, vir_chan_b)


def get_det_ir_pet(chan_hdr):
    """
    convert raw timing data to detector data to pixel-exposure-time (IR)
    """
    # The layout of the detector command word for channels 6--8
    #  14 bits: exposure time factor (ETF)
    #       ETF >= 1: pet = etf * 62.5 ms * ratio
    #       ETF == 0: pet = 31.25 ms
    #   2 bits: mode
    #       0: Normal Mode
    #       1: Hot Mode
    #       2: Test Mode
    #       3: ADC calibration
    #   2 bits: comp. mode, sets the offset compensation
    #       0: no offset compensation
    #       1: store offset compensation
    #       2: use stored offset
    #       3: continuous offset
    #   3 bits: not used
    #   3 bits: fine bias settings [mV]
    #       (-16, 10, -5, -3, -2, -1, 0, 2)
    #   2 bits: not used
    #   4 bits: short pixel exposure time for Hot mode
    #       pet = 28.125e-6 * 2^x  with x <= 10
    #   2 bits: control
    #       1: restart of readout cycle
    #       3: hardware reset of detector module electronics
    #
    command = BitArray(uintbe=chan_hdr['command'], length=32)
    etf = int(command.bin[0:14], 2)
    mode = int(command.bin[14:16], 2)
    spet = int(command.bin[26:30], 2)

    # hot mode
    if mode == 1:
        return 28.125e-6 * 2 ** min(spet, 10)

    # normal mode
    if etf == 0:
        return 1 / 32

    return etf / 16


def mjd_to_datetime(state_id, det_isp):
    """
    Calculates datetime at end of each integration time
    """
    # BCPS enable delay per instrument state
    ri_delay = (0,
                86, 86, 86, 86, 86, 86, 86, 86, 86, 86,
                86, 86, 86, 86, 86, 86, 86, 86, 86, 86,
                86, 86, 86, 86, 86, 86, 86, 86, 86, 86,
                86, 86, 86, 86, 86, 86, 86, 86, 86, 86,
                86, 86, 86, 86, 86, 86, 86, 86, 86, 86,
                86, 86, 86, 86, 86, 86, 86, 86, 111, 86,
                303, 86, 86, 86, 86, 86, 86, 86, 111, 303)

    # Add BCPS H/W delay (92.765 ms)
    _ri = 0.092765 + ri_delay[state_id] / 256

    # the function datetime.timedelta only accepts Python integers
    mst_time = np.full(det_isp.size, np.datetime64('2000', 'us'))
    for ni, dsr in enumerate(det_isp):
        days = int(dsr['mjd']['days'])
        secnds = int(dsr['mjd']['secnds'])
        musec = int(dsr['mjd']['musec']
                    + 1e6 * (dsr['chan_data']['hdr']['bcps'][0] / 16 + _ri))
        mst_time[ni] += np.timedelta64(timedelta(days, secnds, musec))

    return mst_time
