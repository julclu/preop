#!/bin/csh -f 

## in this script we want to 
## 0. Create the argument parser
## 1. import a single bnum and tnum 
## 2. run the archive_exam perl script on it 
##    e.g archive_exam -e E7641/ --raw_prefix t --study po1_preop_recur
## 3. run dcm_qr perl script on it in its new location 

## first arg: bnum
## second arg: tnum 

set bnum = $1
set tnum = $2 

set preop_path_root = "/data/bioe4/po1_preop_recur/"
set recgli_path_root = "/data/RECglioma/archived"

cd $preop_path_root
cd $bnum 
cd $tnum 

set Enum = `ls -d E*`
set pwd = 'cjUCSF\!1'
/home/sf673542/scripts_preop/archive_exam.pl -e $Enum --raw_prefix t --study po1_preop_recur -p $pwd 

cd recgli_path_root 
set Snums = `dcm_exam_info -{$tnum} | grep 'MRImageStorage' | awk '{print$1}'`

@ i = 1
@ num_Snums = `echo ${#Snums}`
while($i <= $num_Snums)
    dcm_qr -${tnum} -p $pwd -s $Snums[$i]
    @ i = $i + 1 
end

