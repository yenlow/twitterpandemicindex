import numpy as np
import re
from datetime import datetime, date
from dateutil.parser import parse

x = '2020.01.23'
def clean_date(x, missing=None, validStart=None, validEnd=None):
    x = str(x)

    # Take the first date if range
    if re.match(r"^\d{2}.\d{2}.\d{4}[\s]*-",x):
        x = x.split('-')[0]

    # strip white spaces
    x = re.sub(r"\s", "", x)  #strip white spaces

    if missing is not None:
        #substitute all others (not in DD.MM.YYYY) with default missing value
        x = re.sub(r"^(?!\d{1,2}.\d{1,2}.\d{2,4}).*$", missing ,x)

    if x=='':
        return None

    elif re.match(r"^\d{1,2}.\d{1,2}.\d{2,4}$", x):
        x = parse(x, dayfirst=True).date()   #smarter date parser supports single digit day or month
#        x = datetime.strptime(x, '%d.%m.%Y').date()  #day, month can't be single digits!

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
    x = str(x)

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
        x = int(x)
        # blank out impossible age
        if 0<x<120:
            return x
        else:
            return None
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


def recode(s,mapdict,inplace=False, missing=None, not_in_mapdict='missing'):
    # if not in mapdict, everything else is coded as missing
    s1 = s.str.strip().replace(mapdict, inplace=inplace)
    if not_in_mapdict!='asis':
        s1[~(s1.isin(mapdict.values()))] = missing
    return s1
