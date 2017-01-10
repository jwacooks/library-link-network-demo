#!/usr3/bustaff/jwa/anaconda_envs/py3/bin/python

## buildLookup.py
## Jack Ammerman
## January 10, 2017

## run this from a working directory
## import required modules
import requests
from urllib.request import Request, urlopen
import sqlite3
import time
import json
import os
import gzip
# first we get the latest json file from wikidata
from pathlib import Path
## check to see if we have the wikidata database. If not, download it
## it is a big file, so we download it in chunks
wikidata_dump = Path("latest-all.json.gz")
if not wikidata_dump.is_file():
    # file doesn't exist
    url = 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz'
    file_name = url.split('/')[-1]
    request = Request(url)

    u = urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(u.getheader('Content-Length'))
    print("Downloading: %s Bytes: %s" % (file_name, file_size))

    file_size_dl = 0
    block_sz = 8192
    counter = 0
    rep = int(file_size/1000000)
    print(rep)
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        counter += 1
        if counter == rep:
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            print(status)
            counter = 0

    f.close()

## define the properties we want to get from the Wikidata file
## the key names for the p_dict dictionary are the Wikidata Property names that I want to include in the lookup datable.
## the value for each is a tuple containing the Name of the source and the base url for the source
p_dict = {}
p_dict['P846'] = ('Global Biodiversity Information Facility ID','http://www.gbif.org/species/$1')
p_dict['P1566'] = ('GeoNames ID','http://sws.geonames.org/$1')
p_dict['P830'] = ('Encyclopedia of Life ID','http://eol.org/pages/$1')
p_dict['P214'] = ('VIAF ID','https://viaf.org/viaf/$1')
p_dict['P345'] = ('IMDb ID','https://tools.wmflabs.org/wikidata-externalid-url/?p=345&url_prefix=http://www.imdb.com/&id=$1')
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


## open the sql connection, create the table
conn = sqlite3.connect('wikidata.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()
create_string = 'CREATE TABLE wiki (lcnum text, '
try:
    c.execute('''DROP TABLE wiki''')
except Exception as e:
    pass
## using the dictionary, we build the rest of the string to create the table. 
for k,v in p_dict.items():
    create_string += (k + ' text, ')
create_string = create_string[:-2] + ')'
## and here we execute the string to create the table
c.execute(create_string)


lc_dict = {}
## specify the datafile
start = time.time()

## reading the wikidata file line by line, 
with gzip.open("latest-all.json.gz") as infile:
    counter = 0
    q = 0
    r = []
    for line in infile:
        counter += 1
        if counter%1000 == 0:
            print(counter)
        try:
            if counter > 1:
                ## we load each line as a json object
                line = line[:-2]
                j = json.loads(line.decode())
                ## we initialize a command string, and with the values in the json object, we add the desired fields to the wiki table
                command_str = "INSERT INTO wiki ("
                if 'Q' in j['id']: 
                    #print()
                    #print(counter)
                    q += 1
                    wd = j['id']
                    #print('wd',wd)
                    command_str += 'wd'
                    val_str = "VALUES ('" + wd +"'"
                    vals = (wd)
                    try:
                        wp = j['sitelinks']['enwiki']['title']
                        command_str += ',wp'
                        val_str += ",'"+ wp +"'" 
                        
                        #print('wp',wp)
                    except Exception as e:
                        #print(e)
                        wp = ''
                        pass
                    for k,v in p_dict.items():
                        try:
                            key = k
                            val = j['claims'][k][0]['mainsnak']['datavalue']['value']
                            #print(key,p_dict[key][0],p_dict[key][1],val)
                            command_str += ','+key
                            val_str += ",'"+ val +"'" 
                            
                        except Exception as e:
                            key = k
                            val = ''
                            pass
                    #print(command_str+ ')',val_str+')')
                    command_str += ')'+val_str+')'
                    c.execute(command_str)
                    conn.commit()
        except Exception as e:
            #print(e)
            pass
# create an index on the LC authority number
s = "CREATE INDEX Idx1 ON wiki(244)"
c.execute(create_string)
conn.close()