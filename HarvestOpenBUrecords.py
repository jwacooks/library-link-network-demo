#!/usr3/bustaff/jwa/anaconda_envs/py3/bin/python

## HarvestOpenBUrecords.py
## Jack Ammerman
## January 10, 2017

## run this from a working directory with 
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import time
import xml.etree.ElementTree as ET
import io
import os
import string
import tarfile
import glob
from pathlib import Path

## first we check to see if there is a publish directory into which to put the file to upload to Zepheira
publish_dir = Path("publish")
if not publish_dir.is_dir():
    os.mkdir('publish')
    print('Created publish directory ' )

## remove any previous output files
try:
    os.remove(' publish/bu_OpenBU_RecordsForZepheria.tar.gz')
except Exception as e:
    pass

## We are harvesting all available collections in OpenBU.
## define the request to begin the OAI-PMH harvest 
## note that we harvest using a 'marc' metadata format
## if you harvest in 'oai_dc' you will need to crosswalk the 
## records to a marcxml format before loading to Zepheira

## first we create an xml element called 'collection' that will be our root in the xml file
collection = ET.Element('collection')
## The records harvested are returned in an xml format that references two schema. 
## the dictionary below defines the two schema, allowing me to use key (ns0 or ns1) in 
## searching the ET.find and ET.findall statements
ns = {'ns0':'http://www.openarchives.org/OAI/2.0/','ns1':'http://www.loc.gov/MARC21/slim'}
##
## this base_url can be modified to point to the repository you wish to harvest
##
base_url = 'http://open.bu.edu/oai/request?verb=ListRecords&metadataPrefix=marc'
request = Request(base_url)
response_body = urlopen(request).read()
## we get the response body and clean up the response string before converting it to an xml object
resp =response_body.decode().replace('\n','').replace('  ',' ')
resp = resp.replace('xmlns="http://www.loc.gov/MARC21/slim"','').replace('xmlns:doc="http://www.lyncode.com/xoai"','')
resp = resp.replace('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"','')
resp = resp.replace('xmlns:dcterms="http://purl.org/dc/terms/"','')
resp = resp.replace('xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"','')
resp = resp.replace('xmlns="http://www.openarchives.org/OAI/2.0/"','')
resp = resp.replace('xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"','')
## here we convert the response to an xml object
response_tree = ET.fromstring(resp)
## we get a list of all the records
oaiRecords = response_tree.findall('.//ListRecords/record/metadata/record')
## we look for the resumtion token, the total number of records to be harvested, and the total returned per request
resumptionToken = response_tree.find('*/resumptionToken',ns)
num_recs = int(resumptionToken.attrib['completeListSize'])
num_in_batch = len(oaiRecords)

try:
    rt = resumptionToken.text
except Exception as we:
    pass
if type(rt) != str:
    rt = 'stop'
    
## we do a little processing of the records, creating an '856' tag from the value in the '024' tag
## then we append as a child of the 'collection' element
for x in range(0,num_in_batch):
    rec = oaiRecords[x]
    _024s = rec.findall('./datafield/[@tag="024"]')
    for _024 in _024s:
        _suba = _024.find('./subfield/[@code="a"]')
        try:
            if 'http' in _suba.text:
                _856 = ET.Element('datafield')
                _856.attrib = {'tag':'856'}
                _subu = ET.Element('subfield')
                _subu.attrib = {'code':'u'}
                _subu.text = _suba.text
                _856.append(_subu)
                rec.append(_856)
            else:
                pass
        except Exception as e:
            pass
    collection.append(rec)
num_in_collection = len(collection)
print(num_in_collection)

## We now use the resumptionToken (rt) and place a request for each group of the remaining records to be harvested
## the process of modifying and appending the records is the same as described above.
while 'marc' in rt:
    time.sleep(1)
    base_url = 'http://open.bu.edu/oai/request?verb=ListRecords&resumptionToken=' + rt
    request = Request(base_url)
    response_body = urlopen(request).read()
    resp =response_body.decode().replace('\n','').replace('  ',' ')
    resp = resp.replace('xmlns="http://www.loc.gov/MARC21/slim"','').replace('xmlns:doc="http://www.lyncode.com/xoai"','')
    resp = resp.replace('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"','')
    resp = resp.replace('xmlns:dcterms="http://purl.org/dc/terms/"','')
    resp = resp.replace('xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"','')
    resp = resp.replace('xmlns="http://www.openarchives.org/OAI/2.0/"','')
    resp = resp.replace('xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"','')
    response_tree = ET.fromstring(resp)
    oaiRecords = response_tree.findall('.//ListRecords/record/metadata/record')
    try:
        rt = response_tree.find('*/resumptionToken',ns).text
    except Exception as e:
        pass
    if type(rt) != str:
        rt = 'stop'
    num_in_batch = len(oaiRecords)
    print(rt)
    for x in range(0,num_in_batch):
        rec = oaiRecords[x]
        _024s = rec.findall('./datafield/[@tag="024"]')
        for _024 in _024s:
            _suba = _024.find('./subfield/[@code="a"]')
            try:
                if 'http' in _suba.text:
                    _856 = ET.Element('datafield')
                    _856.attrib = {'tag':'856'}
                    _subu = ET.Element('subfield')
                    _subu.attrib = {'code':'u'}
                    _subu.text = _suba.text
                    _856.append(_subu)
                    rec.append(_856)
                else:
                    pass
            except Exception as e:
                pass
        collection.append(rec)
##
## after we have harvested all of the records from OpenBU, we create a file and write the collection object 
## out as a marcxml file
out = io.open('bu_openBU_records.xml','wb')
out.write(b'<?xml version="1.0" encoding="UTF-8"?>')
out.write(ET.tostring(collection))
out.close()
## Here we tarzip the OpenBU xml file and place it in the publish directory
tar = tarfile.open('publish/bu_OpenBU_RecordsForZepheria.tar.gz','w:gz')
tar.add('bu_openBU_records.xml')
tar.close()
## finally we remove the marcxml file and end
os.remove('bu_openBU_records.xml')

