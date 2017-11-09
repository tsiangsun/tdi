import urllib2, re, calendar, sys
from multiprocessing import Pool

months = map(lambda x:x.upper(), calendar.month_abbr)
temp_dict = {}

def process_single(x):
    (get_year, get_month) = x
    ret_dict = {}
    url = "http://www.erh.noaa.gov/pbz/hourlywx/hr_pit_{0:0>2d}.{1:0>2d}".format(get_year, get_month)
    print >>sys.stderr, url
    for line in urllib2.urlopen(url):
        m=re.search(r'(\d+)([AP]M) E[SD]T ([A-Z]+) (\d+) (\d+) [^\d]* (\d+)', line)
        if m:
            (hour_12, apm, month_text, day, year, temp) = m.groups()
            if apm=="PM":
                hour = int(hour_12)%12 + 12
            else:
                hour = int(hour_12)%12
            month = months.index(month_text.upper())
            (year, month, day, hour, temp) = map(int, (year, month, day, hour, temp))
            ret_dict[(year,month,day,hour)] = temp
    return ret_dict

workers = Pool(20)
dicts = workers.map(process_single, [(get_year, get_month) for get_year in xrange(1, 14) for get_month in xrange(1,12+1)])

temp_dict = {}
for d in dicts:
    temp_dict.update(d)

for k in sorted(temp_dict):
    (year, month, day, hour) = k
    v = temp_dict[k]
    print "\"%s-%s-%s %s:00:00\",%s"%(year,month,day,hour,v)
