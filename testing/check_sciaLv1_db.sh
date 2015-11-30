#!/bin/bash
#

SCIA_POOL="/SCIA/LV1_01 /SCIA/LV1_02 /SCIA/LV1_03 /SCIA/LV1_04 /SCIA/LV1_05 /SCIA/LV1_06 /SCIA/LV1_07"

for dir in $SCIA_POOL
do
    vdirList=$(ls -1 $dir)
    for vdir in $vdirList
    do
	[ ! -d $dir/$vdir ] && continue
	echo $dir/$vdir
	
	fileList=$(ls -1 $dir/$vdir)
	for file in $fileList
	do
	    prod=$(basename $file ".gz")
	    db_entry=$(inquire_scia.py name $prod)
	    [ "$db_entry" != $dir/$vdir/$file ] && \
		echo "$dir/$vdir/$file: missing"
	done
    done
done

exit 0
