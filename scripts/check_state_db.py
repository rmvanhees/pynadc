import h5py
import numpy as np

with h5py.File('scia_state_settings.h5', 'r') as fid:
    for state_id in range(1, 71):
        grp = fid['State_{:02d}'.format(state_id)]
        mtbl = grp['metaTable'][:]
        clus = grp['clusDef'][:]

        num_clus = 0
        for ni in range(clus.shape[0]):
            if np.array_equal(np.where((clus == clus[ni, :]).all(axis=1))[0],
                              [ni]):
                num_clus += 1

        if clus.shape[0] != num_clus:
            print(state_id, np.sum(mtbl['type_clus'] != 0xFF),
                  clus.shape[0], num_clus)
        else:
            print(state_id, np.sum(mtbl['type_clus'] != 0xFF), num_clus)

flname = '/SCIA/share/nadc_tools/nadc_clusDef_lv1b.h5'
with h5py.File(flname, 'r') as fid:
    for state_id in range(1, 71):
        grp = fid['State_{:02d}'.format(state_id)]
        mtbl = grp['metaTable'][:]
        if 'clusDef' not in grp:
            continue
        clus = grp['clusDef'][:]

        num_clus = 0
        for ni in range(clus.shape[0]):
            if np.array_equal(np.where((clus == clus[ni, :]).all(axis=1))[0],
                              [ni]):
                num_clus += 1

        if clus.shape[0] != num_clus:
            print(state_id, np.sum(mtbl['indx_Clcon'] != 0xFF),
                  clus.shape[0], num_clus)
        else:
            print(state_id, np.sum(mtbl['indx_Clcon'] != 0xFF), num_clus)
