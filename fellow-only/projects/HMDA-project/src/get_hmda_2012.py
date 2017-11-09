import urllib2, urllib, pprint
from bs4 import BeautifulSoup

url_base="http://www.ffiec.gov/hmda"
target = "data/raw/hmda"

def do_link(category, a):
    url_rel = a['href']
    text = a.text.replace(" ","-")
    if text.startswith("By-"):
        return
    if text.startswith("MSA-Office") and "Office" not in url_rel:
        url_rel=url_rel.replace("Description", "Office")
    outfile = url_rel.split("/")[-1].replace(" ", "_")
    ext = outfile.split(".")[-1]
    print 'if [ ! -e "%s/%s" ]; then ' % (target, outfile)
    print '    curl %s/%s > "%s/%s"' % (url_base, urllib.quote(url_rel), target, outfile)
    print 'fi'

def do_table(category, table):
    for row in table.find_all('tr'):
        link = row.find('a')
        if link is not None:
            do_link(category, link)

def do_category(category):
    url="http://www.ffiec.gov/hmda/%sflat.htm" % (category)
    page = urllib2.urlopen(url)
    soup = BeautifulSoup(page, "lxml")
    for table in soup.select('div.contentbox-noshading > table table')[0:3]:
        do_table(category, table)


##
print "set -e"
print "if [ ! -d data/ ]; then echo 'Error: data/ not found.'; exit 1; fi;"
print "if [ ! -d {target}/ ]; then mkdir {target}/; fi;".format(target=target)
print "set -e"
for category in ["pmic" , "hmda"]:
    do_category(category)
    
