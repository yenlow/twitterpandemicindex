# Download dailies and concatenate into big file and upload to dropbox

import pandas as pd
from utils.utils import *

# Read dailies gz (2020-01-27 to 2020-03-21)
dt_range_gz = pd.date_range(start="2020-01-27",end="2020-03-21").astype(str).to_list()
df = None
for d in dt_range_gz:
    filename = f'data/tweet_results/dailies/top1000-{d}_clean-1gram.csv.gz'
#    print(filename)
    df_new = pd.read_csv(filename, sep=",", names=['term','tweet_volume'])
    df_new['date'] = d
    df_new['date'] = df_new['date'].astype('datetime64[ns]')

    if not isinstance(df, pd.DataFrame):
        df = df_new
    else:
        df = pd.concat([df,df_new], ignore_index=True, axis=0)


# Read dailies from github
dt_range = pd.date_range(start="2020-03-22",end="2020-04-27").astype(str).to_list()

for d in dt_range:
    url = f'https://raw.githubusercontent.com/thepanacealab/covid19_twitter/master/dailies/{d}/{d}_top1000terms.csv'
#    print(url)
    df_new = pd.read_csv(url, sep=",", names=['term','tweet_volume'])
    df_new['date'] = d
    df_new['date'] = df_new['date'].astype('datetime64[ns]')

    if not isinstance(df, pd.DataFrame):
        df = df_new
    else:
        df = pd.concat([df,df_new], ignore_index=True, axis=0)

# check that there are no duplicates per term per date
df_dailies = df[((df.term.notna()) & (df.term.str.len()>2) &
                 ~(df.term.str.match('^[0-9]+$|covid|2019n_*cov|corona|amp$|virus$|one$|[w|c|sh]ould|sa[y|id|ys]$|the$|want|non$|really|everyone|per$|still|via|let$|next|time|due|amid|very|much|many|get|got|also|even$|can$|may$|think|des$|going|like$|est$|[0-9]+[feb|th|st|nd|rd]',na=False)))]
df_dailies[df_dailies.duplicated(['date','term'])]

df_dailies.to_pickle('data/tweet_results/df_dailies.zip')
#df_dailies.to_csv('/Users/yensia-low/Dropbox/transfer/dailies.tsv', sep="\t", index=False)


