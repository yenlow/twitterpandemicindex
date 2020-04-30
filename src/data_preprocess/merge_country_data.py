# Download dailies and concatenate into big file and upload to dropbox

import pandas as pd
from api.ascendAPI import ascendClient
from utils.utils import *

A = ascendClient()
A.login()

# Read JHU cases table joined in ascend
df_cases = A.component2pd('COVID_19_Data_Vault','Google_Cloud','All_Cases___Global')
df_cases.country_region.value_counts().index.tolist()
df_cases.set_index('date',inplace=True,drop=True)
df_cases.rename(columns={'deaths':'death_cnt'},inplace=True)

#tmp = df_cases[(df_cases.country_region=='US') & (df_cases.province_state=='New York')]
df_combined = df_combined.join(df_cases,how='outer')

df_cases.to_json('data/sample_jhu_data_for_mockup.json', orient='table')
