#!/usr/bin/env python

import os
import glob
import argparse 
import subprocess as sub
import shlex 
from subprocess import PIPE, Popen
import itertools
import operator
import re

print('creating argument parser')
########### Create an argument parser 
parser = argparse.ArgumentParser(description='In this script we will import a single bnum/tnum, run the archive_exam perl script, and then dcm_qr to desired location.')
parser.add_argument('bnum', type=str, nargs=1, help='Input the b-number of the patient.')
parser.add_argument('tnum', metavar = 't', type =str, nargs = 1, help = 'Input the t-number of the scan.')
print('parsing the arg')
args = parser.parse_args()
#args = parser.parse_args('b2292 t6929'.split())
bnum = ''.join(args.bnum)
tnum = ''.join(args.tnum)

print('setting path roots')
########### Function for creating pathname and changing to directory name: 
preop_path_root = "/data/bioe4/po1_preop_recur/"
recgli_path_root = '/data/RECglioma/archived/'

print('changing path')
def change_path(pathname_root):
    pathname = pathname_root+bnum+'/'+tnum
    os.chdir(pathname)
change_path(recgli_path_root)

print('setting Enum')
Enum = glob.glob('E*')
Enum = Enum[0]
Snums = os.listdir(Enum)

print('finding dti 1000 and 2000 series')
## splitting by index 
dxit_command = 'dcm_exam_info -'+tnum
dxit_output = sub.check_output(dxit_command, shell=True)
dxit_lines = dxit_output.decode('utf-8').splitlines()
index_to_split = [i for i, s in enumerate(dxit_lines) if '-----' in s]
index_to_split = index_to_split[0]
index_to_split+=1
dxit_lines = dxit_lines[index_to_split:]

dti_keywords = ['dwi', '6', '1000', 'b=1000', 'diffu', 'diffusion', '6dir','dti', '2000', 'b=2000', 'HARDI', '55', 'dir=55', '2250']

lines_with_dti = list()
for keyword in dti_keywords:
    lines = [dxit_line.split()[0] for dxit_line in dxit_lines if re.search(keyword, dxit_line, re.IGNORECASE)]
    lines_with_dti.append(lines)

lines_with_dti = [x for x in lines_with_dti if x != []]

unique_data = [list(x) for x in set(tuple(x) for x in lines_with_dti)]

bnum_for_snum_dict = dict()

for i in range(0, len(unique_data)): 
    change_path(recgli_path_root)
    if unique_data[i][0] in Snums: 
        os.chdir(Enum+"/"+unique_data[i][0])
        dcms = os.listdir()
        dcm_dump_command = "dcmdump "+dcms[0]+" | grep -i '0043,1039'"
        x = sub.check_output(dcm_dump_command, shell =True)
        x = x.decode("utf-8") 
        x = x.split("[")
        bnum_for_snum = x[1].split('\\')[0]
        bnum_for_snum_dict[bnum_for_snum]=unique_data[i][0]

change_path(recgli_path_root)

print("executing process_DTI_brain")
if 'diffusion_b=1000' not in os.listdir(".") and '1000' in bnum_for_snum_dict: 
    process_dti1000_command = "process_DTI_brain "+Enum+"/"+bnum_for_snum_dict['1000']+" "+tnum
    try:
        sub.call(process_dti1000_command, shell = True)
    except Exception as error: 
        print(error)
else: 
    print('diffusion 1000 already processed')

if 'diffusion_b=2000' not in os.listdir('.') and '2000' in bnum_for_snum_dict: 
    process_dti2000_command = "process_DTI_brain "+Enum+"/"+bnum_for_snum_dict['2000']+" "+tnum
    try:
        sub.call(process_dti2000_command, shell = True)
    except Exception as error: 
        print(error)
else: 
    print('diffusion 2000 already processed')





