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
set recgli_path_root = "/data/RECglioma/archived"


echo "number of scans set"

while ($i <= $m)
	set bnum = `echo ${b} | cut -d" " -f$i`
	set tnum = `echo ${t} | cut -d" " -f$i`
	echo $i $bnum $tnum

	## go to correct directory 
	cd $preop_path_root
	cd $bnum
	cd $tnum
	## set Enum and password 
	set Enum = `ls -d E*`
	set pwd = 'cjUCSF\!1'
	## we only want to run archive_exam and dcm_qr if not already archived 
	@ num_lines_dxit = `dcm_exam_info -{$tnum} | wc -l`
	echo "everything set, beginning if statement"
	## if the number of lines is less than 18, we can be confident it hasn't already been archived 
	if ($num_lines_dxit <= 18) then
		## run the archive_exam script without the retreival 
		echo 'beginning archival of exam'
		/home/sf673542/scripts_preop/archive_exam.pl -e $Enum --raw_prefix t --study po1_preop_recur -p $pwd 
		echo 'archive exam completed'
		## now go to the correct path for bringing it down 
		cd recgli_path_root 
		## set Snums that we want to pull down 
		set Snums = `dcm_exam_info -{$tnum} | grep 'MRImageStorage' | awk '{print$1}'`
		@ k = 1
		@ num_Snums = `echo ${#Snums}`
		@ num_Snums = $num_Snums - 5 
		## loop through those Snums and pull them down from the archive 
		while($k <= $num_Snums)
	    	dcm_qr -${tnum} -p $pwd -s $Snums[$k]
	    	@ k = $k + 1 
		end
		@ i = $i + 1 
	## otherwise, we want to skip this bnum/tnum 
	else 
		@ i = $i + 1
	endif
end