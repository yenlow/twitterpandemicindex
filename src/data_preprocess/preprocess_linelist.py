# Code to preprocess linelist data
# https://github.com/yenlow/nCoV2019/blob/master/src/data_clean.py
# Adapted from R in https://github.com/beoutbreakprepared/nCoV2019

import pandas as pd
import matplotlib.pyplot as plt
from data_preprocess.utils_cleanlinelist import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.mode.use_inf_as_na = True

# linelist data from UWash IHME collating most other sources
df = pd.read_csv('https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv')

# Sample symptoms for annotating
df.symptoms[df.symptoms.notnull()].tolist()
df.symptoms[df.symptoms.notnull()].tolist()

df.date_death_or_discharge.value_counts()
df.country.value_counts()

# get column data types
print(df.dtypes)

col_date = list(filter(lambda x:'date' in x, df.columns))
col_date.remove("travel_history_dates")
col_admin = list(filter(lambda x:'admin' in x, df.columns))
col_float = ['age','latitude','longitude']
col_bin = ['sex','chronic_disease_binary','travel_history_binary']
col_cat = ['city','province','country','geo_resolution','location','lives_in_Wuhan',
           'outcome','reported_market_exposure','sequence_available']    #drop 'country_new'
col_str = col_admin + ['ID','chronic_disease','symptoms',
            'travel_history_location','source',
            'notes_for_discussion','additional_information','data_moderator_initials']

########### CODE BELOW NOT TESTED SINCE FEB #########
####### DATES #############
# Clean dates
# Only date_confirmation can be filled in with an estimate of ~01-01-2020 at this point
#df['date_confirmation'].fillna('01.03.2020')

# The other dates should be left None if missing
for c in ['date_onset_symptoms', 'date_admission_hospital', 'date_death_or_discharge']:
    #print(df[c].value_counts(dropna=False).sort_index())
    df[c] = df[c].apply(clean_date, missing='', validStart='01-08-2019')
    print(df[c].value_counts(dropna=False).sort_index())

###### NUMERIC ###########
# Clean numeric values and plot histograms
# Clean age
df.age.value_counts(dropna=False)
df.age = df.age.apply(clean_age)
df.age.value_counts(dropna=False)
df.age.hist(bins=100)
plt.show()   #mean approx 50 yrs

# Clean latitudes, longitudes
for c in ['latitude','longitude']:
    df[c] = df[c].apply(clean_float)
    print(df[c].value_counts(dropna=False).sort_index())
    df[c].hist(bins=100)
    plt.show()


###### BINARY, CATEGORICAL ###########
# Get freq counts for col_bin
for c in col_bin:
    print("Before cleaning:")
    print(df[c].value_counts())
    df[c] = df[c].apply(clean_bin, missing=0)
    print("After cleaning:")
    print(df[c].value_counts(dropna=False).sort_index())
#    df[c].hist(bins=100)
#    plt.show()

# Before  cleaning
for c in col_cat:
    print(df[c].value_counts(dropna=False).sort_index())

# Recode into fewer, more meaningful categories
df.sequence_available = recode(df.sequence_available,
                                { "yes": 1}
                                , inplace=False, missing=0).astype('int')

df.reported_market_exposure = recode(df.reported_market_exposure,
                                    {   "yes": 1,
                                        "no": 0,
                                        "yes, retailer in the seafood wholesale market": 1,
                                        "working in another market in Wuhan" : 1,
                                        '18.01.2020 - 23.01.2020': 1,
                                        '18.01.2020 - 23.01.2019': 1}
                        , inplace=False, missing=0).astype('int')

df.outcome = recode(df.outcome,
                    { "died": 'died',
                     "discharged": 'discharged',
                     'stable':'ongoing',
                     'Symptoms only improved with cough. Currently hospitalized for follow-up.': 'ongoing'}
                    , inplace=False, missing='ongoing')

#After cleaning
for c in col_cat:
    print(df[c].value_counts(dropna=False).sort_index())


# Recode to new variables
df['wuhan'] = 1-df['wuhan(0)_not_wuhan(1)'].astype('int')
#df.drop(columns='wuhan(0)_not_wuhan(1)')
df['died'] = recode(df.outcome,
                    { "died": 1}
                    , inplace=False, missing = 0).astype('int')
df['male'] = recode(df.sex, {"female": 0, "male": 1}, inplace=False, missing=np.nan).astype('float')
df['china'] = recode(df.country, {"China": 1}, inplace=False, missing=0).astype('int')
df['e_asia'] = recode(df.country,
                    {"China": 1,
                     "Japan": 1,
                     "Singapore": 1,
                     "Thailand": 1,
                     "South Korea": 1,
                     "Malaysia": 1,
                     "Vietnam": 1,
                     "Taiwan": 1,
                     "Philippines": 1},
                    inplace=False, missing=0).astype('int')

# Check for missingness and whether appropriate as features
print(df.age.value_counts(dropna=False).sort_index())    #95% missing
print(df.male.value_counts(dropna=False).sort_index())   #95% missing
print(df.wuhan.value_counts(dropna=False).sort_index())
print(df.china.value_counts(dropna=False).sort_index())
print(df.e_asia.value_counts(dropna=False).sort_index())
print(df.chronic_disease_binary.value_counts(dropna=False).sort_index())
print(df.died.value_counts(dropna=False).sort_index())   #0.3%

# mortality rate (0.28%)
df['died'].sum()/df.shape[0]*100

# Calculate features from dates
print(col_date)
df['days_onset_outcome']= pd.to_timedelta(df['date_death_or_discharge'] - df['date_onset_symptoms']) / np.timedelta64(1, 'D')
df['days_onset_confirm'] = (df['date_confirmation'] - df['date_onset_symptoms']) / np.timedelta64(1, 'D')
df['days_hosp'] = (df['date_death_or_discharge'] - df['date_admission_hospital']) / np.timedelta64(1, 'D')
df['days_admin_confirm'] = (df['date_confirmation'] - df['date_admission_hospital']) / np.timedelta64(1, 'D')

col_days = list(filter(lambda x:'days' in x, df.columns))
for c in col_days:
    df[c].plot.hist(bins=50, title=c)
    plt.show()

# write cleaned up  df
print(df.dtypes)
df.to_parquet('../../data/df_linelist.parquet')


# construct subset with complete rows
print(df[df.age.notna()]['male'].value_counts(dropna=False))
print(df[df.male.notna()]['age'].value_counts(dropna=False))
df_complete_subset = df[(df.age.notna()) & (df.male.notna())]
print(df_complete_subset.shape)      #740 rows
print(df_complete_subset.age.value_counts(dropna=False).sort_index())
print(df_complete_subset.male.value_counts(dropna=False).sort_index())


#### impute missing datat
df_imputed = df.copy()

# Manually impute (faster than sklearn.impute - unstable API)
# median imputation for age
df.age.median()   #48 yrs old
df_imputed.age = df.age.fillna(50,inplace=False)
print(df_imputed.age.value_counts(dropna=False).sort_index())

# impute missing gender
male_fraction = df.male.value_counts()[1.0]/df.male.value_counts().sum()            #0.56
df_imputed.male.loc[df.male.isna()] = np.random.choice([0,1], p=[1-male_fraction, male_fraction], size=df.male.isna().sum())
print(df_imputed.male.value_counts(dropna=False).sort_index())
print(df_imputed.male.value_counts()[1.0]/df_imputed.male.value_counts().sum())     #0.55

