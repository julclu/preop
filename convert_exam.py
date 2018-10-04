#!/usr/bin/env python

import os
import glob
import argparse 
import subprocess as sub
import shlex 
from subprocess import PIPE, Popen
import pandas as pd

print('creating argument parser')
########### Create an argument parser 
parser = argparse.ArgumentParser(description='In this script we will import a single bnum/tnum, run the archive_exam perl script, and then dcm_qr to desired location.')
parser.add_argument('bnum', type=str, nargs=1, help='Input the b-number of the patient.')
parser.add_argument('tnum', metavar = 't', type =str, nargs = 1, help = 'Input the t-number of the scan.')
parser.add_argument('--config_file_path', metavar = "C", default = '/data/RECglioma/archived/convert_exam_original.cfg', type = str, nargs = 1, help = 'input the path to the config file')
print('parsing the arg')
args = parser.parse_args()
bnum = ''.join(args.bnum)
tnum = ''.join(args.tnum)
config_file_path = "".join(args.config_file_path)

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
Snums_in_preop = os.listdir(Enum)

print('converting exam')
convert_exam_command = "convert_exam -e "+Enum +" -"+tnum+" -c "+config_file_path
sub.call(convert_exam_command, shell = True)

print("running align_intra")
align_intra_command = "align_intra "+tnum
sub.call(align_intra_command, shell = True)

