# -*- coding: utf-8 -*-

#### IMPORTS 1.0

import os
import re
import scraperwiki
import urllib2
from datetime import datetime
from bs4 import BeautifulSoup

#### FUNCTIONS 1.2

import requests    #  import requests to validate url

def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9QY][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9QY][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    now = datetime.now()
    year, month = date[:4], date[5:7]
    validYear = (2000 <= int(year) <= now.year)
    if 'Q' in date:
        validMonth = (month in ['Q0', 'Q1', 'Q2', 'Q3', 'Q4'])
    elif 'Y' in date:
        validMonth = (month in ['Y1'])
    else:
        try:
            validMonth = datetime.strptime(date, "%Y_%m") < now
        except:
            return False
    if all([validName, validYear, validMonth]):
        return True


def validateURL(url, requestdata):
    try:
        r = requests.post(url, data = requestdata, allow_redirects=True, timeout=20)
        count = 1
        while r.status_code == 500 and count < 4:
            print ("Attempt {0} - Status code: {1}. Retrying.".format(count, r.status_code))
            count += 1
            r = requests.post(url, data = requestdata, allow_redirects=True, timeout=20)
        sourceFilename = r.headers.get('Content-Disposition')

        if sourceFilename:
            ext = os.path.splitext(sourceFilename)[1].replace('"', '').replace(';', '').replace(' ', '')
        else:
            ext = os.path.splitext(url)[1]
        validURL = r.status_code == 200
        validFiletype = ext.lower() in ['.csv', '.xls', '.xlsx']
        return validURL, validFiletype
    except:
        print ("Error validating URL.")
        return False, False


def validate(filename, file_url, requestdata):
    validFilename = validateFilename(filename)
    validURL, validFiletype = validateURL(file_url, requestdata)
    if not validFilename:
        print filename, "*Error: Invalid filename*"
        print file_url
        return False
    if not validURL:
        print filename, "*Error: Invalid URL*"
        print file_url
        return False
    if not validFiletype:
        print filename, "*Error: Invalid filetype*"
        print file_url
        return False
    return True


def convert_mth_strings ( mth_string ):
    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    for k, v in month_numbers.items():
        mth_string = mth_string.replace(k, v)
    return mth_string

#### VARIABLES 1.0

entity_id = "E4303_SHMBC_gov"
urls = ["https://secure.sthelens.net/servlet/localtransparency/LocalTransparency", "https://secure.sthelens.net/servlet/localtransparency/PreviousYears"]
errors = 0
data = []
url="http://example.com"

#### READ HTML 1.0

html = urllib2.urlopen(url)
soup = BeautifulSoup(html, 'lxml')


#### SCRAPE DATA
for url in urls:
    if 'PreviousYears' not in url:
        html = urllib2.urlopen(url)
        soup = BeautifulSoup(html, 'lxml')
        blocks = soup.find('select', attrs = {'id':'Options'})
        options = blocks.find_all('option' )
        for option in options:
            links = option['value']
            if 'csv' in links:
                csvMth = links[:3]
                csvYr = links.split('.')[0][-4:]
                csvMth = convert_mth_strings(csvMth.upper())
                requestdata = {'Options':'{}'.format(links),
                        'loadFile':'Load file',}
                data.append([csvYr, csvMth, url, requestdata])
    else:
        html = urllib2.urlopen(url)
        soup = BeautifulSoup(html, 'lxml')
        years = soup.find('select', id="OptionsYear").find_all('option')
        for year in years:
            year = year['value']
            year_html = urllib2.urlopen('https://secure.sthelens.net/servlet/localtransparency/PreviousYears?filter={}'.format(year))
            year_soup = BeautifulSoup(year_html, 'lxml')
            blocks = year_soup.find('select', attrs={'id': 'Options'})
            options = blocks.find_all('option')
            for option in options:
                links = option['value']
                if 'csv' in links:
                    csvMth = links[:3]
                    csvYr = links.split('.')[0][-4:]
                    csvMth = convert_mth_strings(csvMth.upper())
                    requestdata = {'OptionsYear':year, 'Options':'{}'.format(links),
                                   'loadFile':'Load file'}
                    data.append([csvYr, csvMth, url, requestdata])



#### STORE DATA 1.0

for row in data:
    csvYr, csvMth, url, requestdata = row
    filename = entity_id + "_" + csvYr + "_" + csvMth
    todays_date = str(datetime.now())
    file_url = url.strip()

    valid = validate(filename, file_url, requestdata)

    if valid == True:
        scraperwiki.sqlite.save(unique_keys=['f'], data={"l": file_url, "f": filename, "d": todays_date })
        # print filename
        print 'the scraper needs POST requests to get the spending files'
    else:
        errors += 1

if errors > 0:
    raise Exception("%d errors occurred during scrape." % errors)


#### EOF
