#!/bin/bash
#

GOSAT_POOL="/GOSAT/LV2_01 /GOSAT/LV2_02"

for dir in $GOSAT_POOL
do
    sdirList=$(ls -1 $dir)
    for sdir in $sdirList
    do
	[ ! -d $dir/$sdir ] && continue
	yearList=$(ls -1 $dir/$sdir)
	for year in $yearList
	do
	    [ ! -d $dir/$sdir/$year ] && continue
	    mnthList=$(ls -1 $dir/$sdir/$year)
	    for mnth in $mnthList
	    do
		[ ! -d $dir/$sdir/$year/$mnth ] && continue
		dayList=$(ls -1 $dir/$sdir/$year/$mnth)
		for day in $dayList
		do
		    [ ! -d $dir/$sdir/$year/$mnth/$day ] && continue
		    fileList=($(ls -1 $dir/$sdir/$year/$mnth/$day/*.h5))
		    dbList=($(inquire_gosat.py --dbname sron_gosat.db type tcai_2 --date $year$mnth$day))
		    [ ${#dbList[*]} != ${#fileList[*]} ] && \
			echo "$dir/$sdir/$year/$mnth/$day: ${#fileList[*]} != ${#dbList[*]}"
		done
	    done
	done
    done
done

exit 0

