import numpy as np
import re
from datetime import datetime, date
from dateutil.parser import parse

def clean_date(x, missing='01-01-2020', validStart=None, validEnd=None):
    #standardize date delimiters
    #replace '-', '/', '|' with '-'
    x = re.sub(r'\-|,|\/|\|', '.',  x)

    # recode text date to date
    mapdict = { "end of December 2019": '31.12.2019',
                "end of December": '31.12.2019',
                "early january": '01.01.2020',
                "pre 18-01-2020": '01.01.2020',
                "not sure": '25.01.2020'}
    for k in mapdict.keys():
        x = x.strip().replace(k, mapdict.get(k,None))

    # Take the first date if range
    if re.match(r"^\d{2}.\d{2}.\d{4} - ", x):
        x = x.split(' - ')[0]

    # strip white spaces
    x = re.sub(r"\s", "", x)

    #substitute all others (not in DD.MM.YYYY) with default missing value
    x = re.sub(r"^(?!\d{1,2}.\d{1,2}.\d{2,4}).*$", missing ,x)
    if x=='':
        return None
    elif re.match(r"^\d{1,2}.\d{1,2}.\d{2,4}$", x):
        x = parse(x, dayfirst=True).date()   #smarter date parser supports single digit day or month
#        x = datetime.strptime(x, '%d-%m-%Y').date()  #day, month can't be single digits!

    # Recode Aug-Nov 2020 to Aug-Nov 2019
    if x >= date(2020, 8, 1):
        x = x.replace(year=2019)

    # Recode Jan-Feb 2019 to Jan-Feb 2020
    if x <= date(2019, 2, 28):
        x = x.replace(year=2020)

    # check that date is within some valid time range
    if validStart:
        startDate = datetime.strptime(validStart, '%m.%d.%Y').date()
        if validEnd:
            endDate = datetime.strptime(validEnd, '%m.%d.%Y').date()
        else:
            endDate = date.today()

        if startDate <= x <= endDate:
            return x
        else:
            print("{} is out range".format(x))
            return None
    # if validStart is None, do not check if x is within valid range
    else:
        return x



def clean_age(x):
    # strip white spaces
    x = re.sub(r"\s", "", x)
    # change 20s to 25
    x = re.sub(r"\ds$", "5", x)

    # convert age ranges dd-dd to the mid value
    if re.match(r"\d+-\d+", x):
        x = np.floor(np.asarray(x.split('-'), dtype=int).mean())
    else:
        # drop anything that's not a number or period
        x = re.sub(r"[^\d.]", "", x)
    try:
        return int(x)
    except:
        return None


def clean_float(x):
    if isinstance(x,str):
        # strip non-digits and .
        x = re.sub(r"[^\d|.|-]", "", x)
    try:
        return float(x)
    except:
        return None


def clean_bin(x, missing=None):
    if isinstance(x,str):
        # strip non-digits and .
        x = re.sub(r"\s", "", x)
        if x not in ['0','1']:
            return missing
    try:
        return int(x)
    except:
        return None


def recode(s,mapdict,inplace=False, missing=None):
    s1 = s.str.strip().replace(mapdict, inplace=inplace)
    s1[~(s1.isin(mapdict.values()))] = missing
    return s1
