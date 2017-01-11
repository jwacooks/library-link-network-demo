#!/usr3/bustaff/jwa/anaconda_envs/py3/bin/python

## enhance_bib_records_with_linked_data_sourcces.py
## Jack Ammerman
## January 10, 2017

## run this from a working directory
## multi threaded version
## import required modules

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import time
import xml.etree.ElementTree as ET
import io
import os
import string
import tarfile
import glob
import json
import sqlite3
import apsw
from sqlitedict import SqliteDict
from lxml import etree
import sys
counter = 0
from multiprocessing import Process, Queue
from queue import Empty
import multiprocessing
num_workers = multiprocessing.cpu_count()
total_records = 0 
i = 0 
counter = 0
counts = {}

def enhance_field(field):
    # fields in the sqlite database
    # (lc,wd,wp,viaf,att,kdg,jeid,ceid,dpla,europeana,orcid,britannica)
    global counts
    global p_dict
    sub0s = field.findall('.//subfield/[@code="0"]')
    lc = ''
    mms_id = ''
    wikidata_base = 'https://www.wikidata.org/wiki/'
    wikipedia_base='https://en.wikipedia.org/wiki/'
    for sub0 in sub0s:
        ## first we strip the subfields with resolver/wikidata
        if 'resolver/wikidata' in sub0.text:
            field.remove(sub0)
        ## we remove the '(uri) ' prefix
        if '(uri) ' in sub0.text:
            text = sub0.text
            sub0.text = text.replace('(uri) ','')
        ## now we look for an lc authority number
        if 'authorities/names/' in sub0.text:
            lc = sub0.text[sub0.text.index('/names/')+7:]
            #print('assigned: ',lc)
        if 'authorities/subjects/s' in sub0.text:
            lc = sub0.text[sub0.text.index('/subjects/')+10:]
            #print('assigned: ',lc)
        ## check to see if there is a VIAF
        if 'viaf.org/viaf/sourceID/LC' in sub0.text:
            viaf_sub0 = sub0
            

    ## if we found an lc authority number, then we check the lookup table
    
    if len(lc) > 0:
        ## create a sql query string by inserting the LC number
        s = "SELECT * FROM wiki WHERE P244='<<num>>'".replace('<<num>>',lc)
        #conn = sqlite3.connect('./wikidata.db')
        #conn.row_factory = sqlite3.Row
        db_name = './wikidata' + sys.argv[1] + '.db' 
        conn = apsw.Connection(db_name)
        c = conn.cursor()
        record = c.execute(s)
        resp = record.fetchone()
        conn.close()
        subfields = {}

        try:
            for i in range(0,len(resp)):
                try:
                    if len(resp[i]) > 0:
                        if i == 8:
                            viaf_sub0.text = p_dict[i][2].replace('$1',resp[i].replace(' ','_'))
                        else:
                            print(resp[i])
                            print(p_dict[i][2].replace('$1',resp[i].replace(' ','_')))
                            subfields[i] = etree.Element('subfield',code='0')
                            subfields[i].text = p_dict[i][2].replace('$1',resp[i].replace(' ','_'))
                            if resp[i][0] in counts.keys():
                                counts[resp[i][0]] += 1
                            else:
                                counts[resp[i][0]] = 1
                except Exception as e:
                    #print(e)
                    pass            

                
        except Exception as e:
            with open("publish/"+_dir+"loc.txt", "a") as myfile:
                myfile.write(lc)
                myfile.write('\n')
                myfile.close()
            #print(e)
            pass
        for k,v in subfields.items():
            try:
                if len(v.text) > 0:
                    field.append(v)
            except Exception as e:
                #print(e)
                #print(v)
                pass
        
        r = field.getparent()
        mms_id = r.find('./controlfield/[@tag="001"]').text
    sub0s = field.findall('.//subfield/[@code="0"]')
    d_subs = {}
    try:
        for sub0 in sub0s:
            text = sub0.text
            key = text[:text.index('/',9)]
            key = key.replace('https://','').replace('http://','')
            if key in d_subs.keys():
                field.remove(sub0)
            else:
                d_subs[key] = text
    except Exception as e:
        pass
    return(field,mms_id)

def get_recs(file_name):
    global i
    global total_records
    global counter
    global _dir
    ## open the tarfile
    f = tarfile.open(file_name,'r:gz')
    ## get the internal file name
    xml_file_name = f.getnames()[0]
    ## extract the file
    f.extractall()
    ## change the mode of the file
    os.chmod(xml_file_name,0o666)
    ## parse the file and get it's root
    xml = etree.parse(xml_file_name)
    collection = xml.getroot()
    ## make a list of records
    marc_records = collection.findall('./record')
    ## remove the extract file
    os.remove(xml_file_name)
    ## move the tar.gz file to the processed folder
    print(file_name.replace(_dir,'done'))
    os.rename(file_name, file_name.replace(_dir,'done'))
    ## create an output file in the processed directory
    out_name = 'processed/'+_dir+xml_file_name
    ## create a changed file in the changed directory
    changed_name = 'changed/'+_dir+xml_file_name
    ## write the header to these files
    out = io.open(out_name,'wb')
    out.write('<?xml version="1.0" encoding="UTF-8"?><collection>'.encode())
    changed = io.open(changed_name,'wb')
    changed.write('<?xml version="1.0" encoding="UTF-8"?><collection>'.encode())
    #file_num += 1
    ## process each record
    for marc_record in marc_records:#[0:]:
        b_sub0s = marc_record.findall('.//subfield/[@code="0"]')
        if counter%1000 == 0:
            print('\t',counter)
        counter += 1
        d = {}
        for field in marc_record.getchildren():
            ret = enhance_field(field)
            if len(ret[1]) > 0:
                d[ret[1]] = ret[1]
            

        e_sub0s = marc_record.findall('.//subfield/[@code="0"]')  
        if len(e_sub0s) > len(b_sub0s):
            #print('Changed')
            ## if enhanced write the record to the changed file
            changed.write(etree.tostring(marc_record))
            mms_id = marc_record.find('./controlfield/[@tag="001"]').text
            with open("publish/"+_dir+"mms_id.txt", "a") as myfile:
                myfile.write(mms_id)
                myfile.write('\n')
                myfile.close()
        ## write the record to the out file
        out.write(etree.tostring(marc_record))
    ## close the out file
    out.write('</collection>'.encode())
    out.close()
    ## close the changed file
    changed.write('</collection>'.encode())
    changed.close()
    i += 1
    total_records += len(marc_records)
    return(xml_file_name,len(marc_records))

def mp_handler():
    global _dir
    i = 0
    file_num = 0
    p = multiprocessing.Pool(num_workers)
    p.map(mp_worker, file_list)
    ## concatenate files with 10,000 records into files with 100,000 records and tar file
    flist = glob.glob('processed/'+_dir+'*.xml')
    coll = etree.Element('collection')
    for xml_file_name in flist:	            
        if i == 10:
            out_name = 'bu_'+_dir+'_'+str(file_num)+'.xml'
            out = io.open(out_name,'wb')
            out.write('<?xml version="1.0" encoding="UTF-8"?>'.encode())
            out.write(etree.tostring(coll))
            out.close()
            tar = tarfile.open('publish/'+out_name+'.tar.gz','w:gz')
            tar.add(out_name)
            tar.close()
            os.remove(out_name)
            coll = etree.Element('collection')
            i = 0
            file_num += 1
        i += 1
        ## parse the file and get it's root
        xml = etree.parse(xml_file_name)
        collection = xml.getroot()
        ## make a list of records
        marc_records = collection.findall('./record')
        for marc_record in marc_records:
            coll.append(marc_record)

    ## create final output and tar file
    out_name = 'bu_'+_dir+'_'+str(file_num)+'.xml'
    out = io.open(out_name,'wb')
    out.write('<?xml version="1.0" encoding="UTF-8"?>'.encode())
    out.write(etree.tostring(coll))
    out.close()
    tar = tarfile.open('publish/'+out_name+'.tar.gz','w:gz')
    tar.add(out_name)
    tar.close()
    os.remove(out_name)

    return
 
def mp_worker(f):
    #print(" Processsing %s " % f)
    resp = get_recs(f)
    fname = resp[0]
    records = resp[1]
    #tot += records
    print(" Process %s\tComplete. Number of records %s" % resp)
    #print(resp)
    print()
    return
## z_prep.py is called with a command line parameter that is a number that is used to identify the job number and the directory name
## holding the bib records to be enhanced. Below, the _dir variable is the directory name.
_dir = 'e' + sys.argv[1] 
file_list = glob.glob(_dir + '/*.gz')
p_dict = {}
p_dict['P846'] = ('Global Biodiversity Information Facility ID','http://www.gbif.org/species/$1')
p_dict['P1566'] = ('GeoNames ID','http://sws.geonames.org/$1')
p_dict['P830'] = ('Encyclopedia of Life ID','http://eol.org/pages/$1')
p_dict['P214'] = ('VIAF ID','https://viaf.org/viaf/$1')
p_dict['P345'] = ('IMDb ID','http://www.imdb.com/$1')
p_dict['P244'] = ('LCAuth ID','http://id.loc.gov/authorities/$1')
p_dict['P2163'] = ('FAST-ID','http://id.worldcat.org/fast/$1')
p_dict['P727'] = ('Europeana ID','http://data.europeana.eu/item/$1')
p_dict['P1415'] = ('Oxford Biography Index Number','http://www.oxforddnb.com/index/$1/')
p_dict['P245'] = ('ULAN ID','http://vocab.getty.edu/page/ulan/$1')
p_dict['P1871'] = ('CERL ID','https://thesaurus.cerl.org/record/$1')
p_dict['P2036'] = ('African Plant Database','http://www.ville-ge.ch/musinfo/bd/cjb/africa/details.php?langue=an&id=$1')
p_dict['P236'] = ('ISSN','https://www.worldcat.org/issn/$1')
p_dict['P1816'] = ('National Portrait Gallery (London) person ID','http://www.npg.org.uk/collections/search/person/$1')
p_dict['P243'] = ('OCLC control number','http://www.worldcat.org/oclc/$1')
p_dict['P402'] = ('OpenStreetMap Relation identifier','https://www.openstreetmap.org/relation/$1')
p_dict['P1417'] = ('Encyclopaedia Britannica Online ID','https://www.britannica.com/$1')
p_dict['P212'] = ('ISBN-13','https://www.wikidata.org/wiki/Special:BookSources/$1')
p_dict['P1156'] = ('Scopus Source ID','https://www.scopus.com/source/sourceInfo.uri?sourceId=$1')
p_dict['P1157'] = ('US Congress Bio ID','http://bioguide.congress.gov/scripts/biodisplay.pl?index=$1')
p_dict['P957'] = ('ISBN-10','https://www.wikidata.org/wiki/Special:BookSources/$1')
p_dict['P1184'] = ('handle','http://hdl.handle.net/$1')
p_dict['P486'] = ('MeSH ID','https://www.nlm.nih.gov/cgi/mesh/2016/MB_cgi?field=uid&term=$1')
p_dict['P1795'] = ('Smithsonian American Art Museum: person/institution thesaurus id','http://americanart.si.edu/collections/search/artist/?id=$1')
p_dict['P1649'] = ('KMDb person ID','http://www.kmdb.or.kr/eng/vod/mm_basic.asp?person_id=$1')
p_dict['P724'] = ('Internet Archive ID','https://archive.org/details/$1')
p_dict['P1144'] = ('LCOC LCCN (bibliographic)','https://lccn.loc.gov/$1')
p_dict['P1230'] = ('JSTOR journal code','http://www.jstor.org/journal/$1')
p_dict['P2671'] = ('Google Knowledge Graph identifier','https://kgsearch.googleapis.com/v1/entities:search?key=API_KEY&ids=$1')
p_dict['P496'] = ('ORCID','https://orcid.org/$1')
p_dict['P672'] = ('MeSH Code','http://l.academicdirect.org/Medicine/Informatics/MESH/browse/tree/?t=$1')
p_dict['P1960'] = ('Google Scholar ID','https://scholar.google.com/citations?user=$1')
p_dict['P675'] = ('Google Books ID','https://books.google.com/books?id=$1')
p_dict['P3241'] = ('Catholic Encyclopedia ID','http://www.newadvent.org/cathen/$1.htm')
p_dict['P760'] = ('DPLA ID','http://dp.la/item/$1')
p_dict['P1014'] = ('AAT ID', 'http://vocab.getty.edu/page/aat/$1')
p_dict['P1667'] = ('TGN ID', 'http://vocab.getty.edu/page/tgn/$1')
p_dict['P1669'] = ('CONA ID', 'http://vocab.getty.edu/cona/$1')
p_dict['P3123'] = ('Stanford Encyclopedia of Philosophy ID', 'http://plato.stanford.edu/entries/$1')
p_dict['wd'] = ('WikiData','https://www.wikidata.org/wiki/$1')
p_dict['wp'] = ('Wikipedia', 'https://en.wikipedia.org/wiki/$1')
p_dict['P213'] = ('ISNI', 'http://isni.org/isni/$1.rdf')
p_dict[0] = ('wd','WikiData','https://www.wikidata.org/wiki/$1')
p_dict[1] = ('wp','Wikipedia', 'https://en.wikipedia.org/wiki/$1')
p_dict[2] = ('lcnum','LCAuth ID','http://id.loc.gov/authorities/$1')
p_dict[3] = ('P1871','CERL ID','https://thesaurus.cerl.org/record/$1')
p_dict[4] = ('P1156','Scopus Source ID','https://www.scopus.com/source/sourceInfo.uri?sourceId=$1')
p_dict[5] = ('P1816','National Portrait Gallery (London) person ID','http://www.npg.org.uk/collections/search/person/$1')
p_dict[6] = ('P724','Internet Archive ID','https://archive.org/details/$1')
p_dict[7] = ('P1669''CONA ID', 'http://vocab.getty.edu/cona/$1')
p_dict[8] = ('P214','VIAF ID','https://viaf.org/viaf/$1')
p_dict[9] = ('P1667','TGN ID', 'http://vocab.getty.edu/page/tgn/$1')
p_dict[10] = ('P402','OpenStreetMap Relation identifier','https://www.openstreetmap.org/relation/$1')
p_dict[11] = ('P1960','Google Scholar ID','https://scholar.google.com/citations?user=$1')
p_dict[12] = ('P244','LCAuth ID','http://id.loc.gov/authorities/$1')
p_dict[13] = ('P345','IMDb ID','http://www.imdb.com/$1')
p_dict[14] = ('P212','ISBN-13','https://www.wikidata.org/wiki/Special:BookSources/$1')
p_dict[15] = ('P3241','Catholic Encyclopedia ID','http://www.newadvent.org/cathen/$1.htm')
p_dict[16] = ('P760','DPLA ID','http://dp.la/item/$1')
p_dict[17] = ('P2671','Google Knowledge Graph identifier','https://kgsearch.googleapis.com/v1/entities:search?key=API_KEY&ids=$1')
p_dict[18] = ('P1649','KMDb person ID','http://www.kmdb.or.kr/eng/vod/mm_basic.asp?person_id=$1')
p_dict[19] = ('P243','OCLC control number','http://www.worldcat.org/oclc/$1')
p_dict[20] = ('P672','MeSH Code','http://l.academicdirect.org/Medicine/Informatics/MESH/browse/tree/?t=$1')
p_dict[21] = ('P846','Global Biodiversity Information Facility ID','http://www.gbif.org/species/$1')
p_dict[22] = ('P1014''AAT ID', 'http://vocab.getty.edu/page/aat/$1')
p_dict[23] = ('P3123','Stanford Encyclopedia of Philosophy ID', 'http://plato.stanford.edu/entries/$1')
p_dict[24] = ('P236','ISSN','https://www.worldcat.org/issn/$1')
p_dict[25] = ('P727','Europeana ID','http://data.europeana.eu/item/$1')
p_dict[26] = ('P830','Encyclopedia of Life ID','http://eol.org/pages/$1')
p_dict[27] = ('P1417','Encyclopaedia Britannica Online ID','https://www.britannica.com/$1')
p_dict[28] = ('P1144','LCOC LCCN (bibliographic)','https://lccn.loc.gov/$1')
p_dict[29] = ('P1157','US Congress Bio ID','http://bioguide.congress.gov/scripts/biodisplay.pl?index=$1')
p_dict[30] = ('P1184','handle','http://hdl.handle.net/$1')
p_dict[31] = ('P486','MeSH ID','https://www.nlm.nih.gov/cgi/mesh/2016/MB_cgi?field=uid&term=$1')
p_dict[32] = ('P2036','African Plant Database','http://www.ville-ge.ch/musinfo/bd/cjb/africa/details.php?langue=an&id=$1')
p_dict[33] = ('P245','ULAN ID','http://vocab.getty.edu/page/ulan/$1')
p_dict[34] = ('P1566','GeoNames ID','http://sws.geonames.org/$1')
p_dict[35] = ('P675','Google Books ID','https://books.google.com/books?id=$1')
p_dict[36] = ('P1795','Smithsonian American Art Museum: person/institution thesaurus id','http://americanart.si.edu/collections/search/artist/?id=$1')
p_dict[37] = ('P1415','Oxford Biography Index Number','http://www.oxforddnb.com/index/$1')
p_dict[38] = ('P957','ISBN-10','https://www.wikidata.org/wiki/Special:BookSources/$1')
p_dict[39] = ('P496','ORCID','https://orcid.org/$1')
p_dict[40] = ('P1230','JSTOR journal code','http://www.jstor.org/journal/$1')
p_dict[41] = ('P2163','FAST-ID','http://id.worldcat.org/fast/$1')
p_dict[42] = ('ISNI', 'http://isni.org/isni/$1.rdf')


if __name__ == '__main__':
 
    print('Number of workers: ',num_workers)
    print('Number of files: ',len(file_list))
    start = time.time()
    mp_handler()
    stop = time.time()
    total_seconds = stop-start
    print('\tminutes: ',str(int(total_seconds/60)))
    print(total_records)
    for k,v in counts.items():
        print(k,v)
    



