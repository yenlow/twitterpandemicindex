# Download dailies and concatenate into big file and upload to dropbox

import pandas as pd
from api.ascendAPI import ascendClient
from utils.utils import *

A = ascendClient()
A.login()

df = pd.read_csv(dailies_url,sep="\t")

# check that there are no duplicates per term per date
df = df[((df.term.notna()) & ~(df.term.str.match('^[0-9]+$',na=False)))]
df[df.duplicated(['date','term'])]
#df.to_csv('/Users/yensia-low/Dropbox/transfer/dailies.tsv', sep="\t", index=False)

df_crosstab = df.pivot(index='date',columns='term', values='tweet_volume')
top_terms = df_crosstab.columns[df_crosstab.max()>20000]
df_crosstab = df_crosstab[top_terms]
#df_crosstab.reset_index().to_csv('/Users/yensia-low/Dropbox/transfer/dailies.tsv', sep="\t", index=False)

# Read google mobility data from ascend
df_daily_mobility_tweet_vol = A.component2pd('yenlow_gmail_com','test','daily_mobility_us_tweet_vol')
df_daily_mobility_tweet_vol.set_index('date',inplace=True,drop=True)
df_combined = df_daily_mobility_tweet_vol.join(df_crosstab,how='outer')


# Example 6: Read table joined in ascend
df_cases = A.component2pd('COVID_19_Data_Vault','Google_Cloud','All_Cases___Global')
df_cases.country_region.value_counts().index.tolist()
df_cases.set_index('date',inplace=True,drop=True)
df_cases.rename(columns={'deaths':'death_cnt'},inplace=True)

tmp = df_cases[(df_cases.country_region=='US') & (df_cases.province_state=='New York')]
df_combined = df_combined.join(tmp,how='outer')

df_combined.to_json('data/sample_data_for_mockup.json', orient='table')
df_cases.to_json('data/sample_jhu_data_for_mockup.json', orient='table')
