#!/bin/bash
#

GOSAT_POOL="/GOSAT/LV1_01 /GOSAT/LV1_02"

for dir in $GOSAT_POOL
do
    sdirList=$(ls -1 $dir)
    for sdir in $sdirList
    do
	[ ! -d $dir/$sdir ] && continue
	vdirList=$(ls -1 $dir/$sdir)
	for vdir in $vdirList
	do
	    [ ! -d $dir/$sdir/$vdir ] && continue
	    yearList=$(ls -1 $dir/$sdir/$vdir)
	    for year in $yearList
	    do
		[ ! -d $dir/$sdir/$vdir/$year ] && continue
		mnthList=$(ls -1 $dir/$sdir/$vdir/$year)
		for mnth in $mnthList
		do
		    [ ! -d $dir/$sdir/$vdir/$year/$mnth ] && continue
		    dayList=$(ls -1 $dir/$sdir/$vdir/$year/$mnth)
		    for day in $dayList
		    do
			[ ! -d $dir/$sdir/$vdir/$year/$mnth/$day ] && continue
			fileList=($(ls -1 $dir/$sdir/$vdir/$year/$mnth/$day/*.01))
			dbList=($(inquire_gosat.py --dbname sron_gosat.db type tfts_1 --obs_mode $sdir --prod_version $vdir --date $year$mnth$day))
			[ ${#dbList[*]} != ${#fileList[*]} ] && \
			    echo "$dir/$sdir/$vdir/$year/$mnth/$day: ${#fileList[*]} != ${#dbList[*]}"
		    done
		done
	    done
	done
    done
done

exit 0

