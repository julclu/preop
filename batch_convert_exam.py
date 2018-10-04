#!/usr/bin/env python

import os
import glob
import argparse 
import subprocess as sub
import shlex 
from subprocess import PIPE, Popen
import pandas as pd

def getting_bnum_tnum_list(bnum_tnum_csv): 
    ########### Get bnum_tnum list: 
    bnum_tnum_csv_root = "/home/sf673542/preop_convert_work/"
    bnum_tnum_df = pd.read_csv(bnum_tnum_csv_root+bnum_tnum_csv, header = None)
    bnum_tnum_df.columns = ['bnum', 'tnum', 'DUMMY']
    return bnum_tnum_df

parser = argparse.ArgumentParser(description='In this script we will import a single bnum/tnum, run the archive_exam perl script, and then dcm_qr to desired location.')
parser.add_argument('bnum_tnum_csv', type=str, nargs=1, help='List of bnums in one column, tnums in the other, the name please.')
parser.add_argument('--config_file_path', metavar = "C", default = '/data/RECglioma/archived/convert_exam_original.cfg', type = str, nargs = 1, help = 'input the path to the config file')

args = parser.parse_args()
bnum_tnum_csv = ''.join(args.bnum_tnum_csv)
config_file_path = ''.join(args.config_file_path)

bnum_tnum_df = getting_bnum_tnum_list(bnum_tnum_csv)

print('executing program')
for index, row in bnum_tnum_df.iterrows():
    bnum = row['bnum']
    tnum = row['tnum']
    print('bnum= '+bnum+'; tnum= '+tnum)
    try: 
        convert_exam_command = "convert_exam.py "+bnum +" "+tnum
        sub.call(convert_exam_command, shell = True)
    except Exception as error:  
        print('convert_exam or align_intra error for bnum='+bnum+'tnum='+tnum)
        print(error)