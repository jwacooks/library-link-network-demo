#!/usr3/bustaff/jwa/anaconda_envs/py3/bin/python

## get_files_and_submit_jobs.py
## Jack Ammerman
## January 10, 2017

## run this from a working directory
## import required modules

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import sys
import time
import xml.etree.ElementTree as ET
import io
import os
import string
import tarfile
import glob
import json
import sqlite3
from sqlitedict import SqliteDict
from lxml import etree
from pathlib import Path
import shutil
import pysftp
import getpass
from subprocess import call

# multi threaded version

def get_files():
    ## first we remove the extract directories
    dir_list = glob.glob("e*")
    for d in dir_list:
        try:
            shutil.rmtree(d)
        except Exception as e:
            return(e)

    # now we make a new extract directory and sftp the files from the ftp server
    os.mkdir('extract')
    ## define ftp host and username from which to get the bib records exported from Alma
    hostname = 'dioa1.bu.edu'
    username = 'alma'
    password = getpass.getpass()
    with pysftp.Connection(hostname,username=username,password=password) as sftp:
        try:
            sftp.get_d('alma/Zepheira','extract')
        except Exception as e:
            return(e)
    return('success')

def distribute_files():
    ## then we check to see if there are extract directories into which to put the files to be processed
    try:
        num = int(sys.argv[1])
    except Exception as e:
        num = 30
    for i in range(1,num+1):
        dir_name = 'e'+str(i)
        e_dir = Path(dir_name)
        try:
            if not e_dir.is_dir():
                os.mkdir(dir_name)
                print('Created ' + dir_name + ' directory')
            else:
                print('Directory: ' + dir_name + ' exists')
        except Exception as e:
            return(e)
    return('success')

def copy_db(number):
    wikidata = Path("wikidata.db")
    if not wikidata.is_file():
        return('no database found')
    for i in range(1,num+1):
        copy_name = 'wikidata' + str(i) + '.db'
        response = call(["cp", "wikidata.db", copy_name])
        if response != 0:
            return('error copying database: ' + copy_name)
    return('success')        
    
def submit_job(number):
    for i in range(1,num+1):
        copy_name = 'wikidata' + str(i) + '.db'
        wikidata = Path(copy_name)
        if not wikidata.is_file():
            return('no database found')
        response = call(["qsub", "enhance_bib_records_with_linked_data_sourcces.py", str(i)])
        if response != 0:
            return('error submitting job: ' + str(i))
    return('success')     

counter = 0
from multiprocessing import Process, Queue
from queue import Empty
import multiprocessing
num_workers = multiprocessing.cpu_count()
total_records = 0 
i = 0 
counter = 0

file_list = glob.glob('extract/*.gz')

if __name__ == '__main__':

    try:
        num = int(sys.argv[1])
    except Exception as e:
        num = 30
    i = 1
    response = get_files()
    print('Get files: ' + response)
    if response == 'success':
        response = distribute_files()
    print('Created directories for distribution: ' + response)
    if response == 'success':
        print('Ready to distribute files')
        file_list = glob.glob('extract/*.gz')
        print('Number of workers: ',num_workers)
        print('Number of files: ',len(file_list))
        for f in file_list:
            #print('e' + str(i) + f[7:])
            os.rename(f,'e' + str(i) + f[7:])
            if i == num:
                i = 1
            else:
                i += 1
    response = copy_db(num)
    response = submit_job(num)

