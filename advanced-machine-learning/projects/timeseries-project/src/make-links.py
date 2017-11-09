## /usr/bin/env python
#
# The script to generate the current list of URLs from which to download gdelt data.
# Does no downloading itself, and the URLs are relative not absolute.
# The rel-base is http://data.gdeltproject.org/events
#
# For instance, to download all current daily files one might run
# src/make-links.py | grep ".export.CSV." | parallel --max-procs 10 curl http://data.gdeltproject.org/events/{} > data/raw/gdelt/{}
#
import urllib2
from bs4 import BeautifulSoup
from tidylib import tidy_document

index_url="http://data.gdeltproject.org/events/index.html"

# On my DO instance I had to use tidy for it to work.  
# It worked fine on my home machine.  More lxml.html shenanigans?
tidy, errors = tidy_document( urllib2.urlopen(index_url).read() )
soup = BeautifulSoup( tidy )

for a in soup.find_all('a'):
    print a['href']
