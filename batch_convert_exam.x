#!/bin/csh -f 

if ($#argv > 1 ) then
    echo "Please enter the path to the .csv file containing the list of"
    echo "bnum/tnum that you'd like to ensure are processed correctly."
    exit(1)
endif

set n = $1
set b = `more $n | cut -d"," -f1`
set t = `more $n | cut -d"," -f2`

@ i = 1

@ m = `echo $n | cut -d"." -f2`

set preop_path_root = "/data/bioe4/po1_preop_recur/"
set recgli_path_root = "/data/RECglioma/archived/"
set config_file_name = "convert_exam_original.cfg"

echo "number of scans set"

while ($i <= $m)
	## set bnum/tnum 
	set bnum = `echo ${b} | cut -d" " -f$i`
	set tnum = `echo ${t} | cut -d" " -f$i`
	
	## keep track of which bnum/tnum we're on
	echo $i $bnum $tnum

	## go to correct directory 
	cd $recgli_path_root
	cd $bnum
	cd $tnum
	set Enum = `ls -d E*`
	set pwd = 'cjUCSF\!1'
	convert_exam -e $Enum -t $tnum -c $recgli_path_root$config_file_name
	
end