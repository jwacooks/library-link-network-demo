#!/usr3/bustaff/jwa/anaconda_envs/py3/bin/python

## upload.py
## Jack Ammerman
## January 10, 2017

## run this from a working directory
## import required modules

import os
import requests
os.chdir('publish')
files = os.listdir('.')
url = 'https://<your_upload_key_and_url>'
for f in files:
    if f[-2:] == 'gz':
        print(f)
        files = {'file': open(f, 'rb')}
        r = requests.post(url, files=files)
        print(r)
