#!/bin/bash
#

SCIA_POOL="/SCIA/Level2/ESA"

for dir in $SCIA_POOL
do
    yearList=$(ls -1 $dir)
    for year in $yearList
    do
	[ ! -d $dir/$year ] && continue
	mnthList=$(ls -1 $dir/$year)
	for mnth in $mnthList
	do
	    [ ! -d $dir/$year/$mnth ] && continue
	    dayList=$(ls -1 $dir/$year/$mnth)
	    for day in $dayList
	    do
		[ ! -d $dir/$year/$mnth/$day ] && continue
		fileList=$(ls -1 $dir/$year/$mnth/$day)
		for file in $fileList
		do
		    prod=$(basename $file ".gz")
		    db_entry=$(inquire_scia.py --dbname sron_scia.db name $prod)
		    [ "$db_entry" != $dir/$year/$mnth/$day/$file ] && \
			echo "$dir/$year/$mnth/$day/$file: missing"
		done
	    done
	done
    done
done

exit 0
