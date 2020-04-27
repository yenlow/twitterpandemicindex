from api.ascendAPI import ascendClient
from utils.utils import *
import pandas as pd

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_colwidth', 50)
pd.set_option('display.width', 2000)

A = ascendClient()
A.login()

# Data service (e.g. COVID_19_Data_Vault) > Data flow (AWS) > Data feed (each box)
A.ls()  #list data services
A.ls('COVID_19_Data_Vault') #list data flows
A.ls('COVID_19_Data_Vault','University_of_Washington') #list components

A.ls('yenlow_gmail_com') #list data flows
A.ls('yenlow_gmail_com','test') #list components

# Example 1: Read US_cases_by_county from COVID_19_Data_Vault.AWS.US_Cases_by_County
A.get_component('COVID_19_Data_Vault','AWS', 'US_Cases_by_County')
df_cases_county = A.component2pd('COVID_19_Data_Vault','AWS', 'US_Cases_by_County')
df_cases_county.shape

# Example 2: Read risk populations by state from yenlow_gmail_com.test.risk_pop
df_riskpop = A.component2pd('yenlow_gmail_com','test','risk_pop')
df_riskpop.shape

# Example 3: Read symptoms line list (patient level with dates)
# code to clean up linelist is in https://github.com/yenlow/nCoV2019/blob/master/src/data_clean.py
df_symptoms_ll = A.component2pd('COVID_19_Data_Vault','University_of_Washington','COVID_19_Cases_w__Symptoms')
df_symptoms_ll.shape


# Example 4: Read US census data
# 2000 from ascend.COVID_19_Data_Vault.US_Census_Bureau_2000_present
# Better to use 2010 from BigQuery
df_census_2000 = A.component2pd('COVID_19_Data_Vault','Google_Cloud','US_Census_Bureau_2000_present')
df_census_2000.shape


# Example 5: Read table joined in ascend
df_daily_mobility_tweet_vol = A.component2pd('yenlow_gmail_com','test','daily_mobility_us_tweet_vol')
df_daily_mobility_tweet_vol.shape
df_daily_mobility_tweet_vol.to_json('data/sample_data_for_mockup.json', orient='table')



# convert csv to csv/json/parquet for uploading to ascend (must be <2MB)
csv2other('../../data/policyactions_state.csv', 'json')
csv2other('../../data/riskpop_state.csv', 'json')
df = csv2other('data/Global_Mobility_Report.csv', 'parquet')





import pandas as pd
header = ['profileId','submissionOrder','age','sex',
          'hasBeenTested','hasBeenInContactWithInfected',
          'symptomStart','testResult','submissionDate',
          'bodyTemperature','smokingHabit','isolationStatus',
          'diagnosedWithOtherConditions',
          'DRY_COUGH','EXHAUSTION','FEVER','HEAVY_BREATHING',
          'MUSCLE_ACHING','DIARRHEA','HEADACHE','SORE_THROAT',
          'NO_TASTE','NO_SMELL','SLIME_COUGH','RUNNY_NOSE','NAUSEA_OR_VOMITING']
len(header)

data = pd.read_csv('https://coronastatus.ro/api/reports/reports.csv', skiprows=1, names=header) # use sep="," for coma separation.
data.describe()


import requests

headers = {
    'accept': 'text/csv',
}

response = requests.get('https://coronastatus.ro/api/reports/reports.csv', headers=headers)
