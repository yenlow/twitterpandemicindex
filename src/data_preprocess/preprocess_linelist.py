# Code to preprocess linelist data
# Adapted from R in IHME's https://github.com/beoutbreakprepared/nCoV2019

import pandas as pd
import matplotlib.pyplot as plt
from data_preprocess.utils_cleanlinelist import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 100)
pd.options.mode.use_inf_as_na = True

# linelist data from UWash IHME collating most other sources
df = pd.read_csv('https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv')

# Sample symptoms for annotating
df.symptoms[df.symptoms.notnull()].tolist()

# get column data types
print(df.dtypes)

col_date = list(filter(lambda x:'date' in x, df.columns))
col_date.remove("travel_history_dates")
col_admin = list(filter(lambda x:'admin' in x, df.columns))
col_float = ['age','latitude','longitude']
col_bin = ['chronic_disease_binary','travel_history_binary']  #sex is bin text not 1/0
col_cat = ['city','province','country','geo_resolution','location','lives_in_Wuhan',
           'outcome','reported_market_exposure','sequence_available']    #drop 'country_new'
col_str = col_admin + ['ID','chronic_disease','symptoms',
            'travel_history_location','source',
            'notes_for_discussion','additional_information','data_moderator_initials']


########### CODE BELOW NOT TESTED SINCE FEB #########
####### DATES #############
# Clean dates and convert to date type
# Dates should be left None if missing
for c in col_date:
    print(df[c].value_counts(dropna=False).sort_index())
    df[c] = df[c].apply(clean_date, validStart='01.08.2019')
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
    df[c].hist(bins=100)
    plt.show()

# Before  cleaning
for c in col_cat:
    print(df[c].value_counts(dropna=False).sort_index())

# Recode into fewer, more meaningful categories
df.lives_in_Wuhan = recode(df.lives_in_Wuhan,
                                { "yes" :1 }
                                , inplace=False, missing=0, not_in_mapdict='missing').astype('int')

df.sequence_available = recode(df.sequence_available,
                                { "yes": 1}
                                , inplace=False, missing=0, not_in_mapdict='missing').astype('int')

df.reported_market_exposure = recode(df.reported_market_exposure,
                                    {   "yes": 1,
                                        "no": 0,
                                        "yes, retailer in the seafood wholesale market": 1,
                                        "working in another market in Wuhan" : 1,
                                        '18.01.2020 - 23.01.2020': 1,
                                        '18.01.2020 - 23.01.2019': 1}
                        , inplace=False, missing=0, not_in_mapdict='missing').astype('int')

df.outcome = recode(df.outcome,
                    {"died": 'died',
                     "Dead": 'died',
                     "Death": 'died',
                     "Deceased": 'died',
                     "discharged": 'recovered',
                     'stable':'recovered',
                     'Alive': 'recovered'}
                    , inplace=False, missing='ongoing', not_in_mapdict='missing')

#After cleaning
for c in col_cat:
    print(df[c].value_counts(dropna=False).sort_index())


# Recode to new variables
#df.drop(columns='wuhan(0)_not_wuhan(1)')
df['died'] = recode(df.outcome,
                    { "died": 1}
                    , inplace=False, missing = 0, not_in_mapdict='missing').astype('int')
df['male'] = recode(df.sex, {"female": 0, "male": 1}, inplace=False, missing=np.nan, not_in_mapdict='missing').astype('float')
df['country_new'] = recode(df.country, {'nan': None,
                                    'Jammu and Kashmir': 'India',
                                    'Jharkhand': 'India',
                                    'Delhi': 'India',
                                    'Puducherry': 'India',
                                    'Rajasthan': 'India',
                                    'Uttar Pradesh': 'India',
                                    'Tamil Nadu': 'India',
                                    'Andhra Pradesh': 'India',
                                    'Maharashtra': 'India',
                                    'Telangana': 'India',
                                    'West Bengal': 'India',
                                    'Uttarakhand': 'India',
                                    'Ladakh': 'India',
                                    'Himachal Pradesh': 'India',
                                    'Odisha': 'India',
                                    'Kerala': 'India'},
                       inplace=False, missing=None, not_in_mapdict='asis')

# Check for missingness and whether appropriate as features
print(df.age.value_counts(dropna=False).sort_index())    #96% missing
print(df.male.value_counts(dropna=False).sort_index())   #96% missing
print(df.chronic_disease_binary.value_counts(dropna=False).sort_index())
print(df.died.value_counts(dropna=False).sort_index())   #0.03%

# mortality rate (0.037%)
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


# construct subset with complete rows (drop the missing 6%)
print(df[df.age.notna()]['male'].value_counts(dropna=False))
print(df[df.male.notna()]['age'].value_counts(dropna=False))
df_complete_subset = df[(df.age.notna()) & (df.male.notna())]
print(df_complete_subset.shape)      #740 rows
print(df_complete_subset.age.value_counts(dropna=False).sort_index())
print(df_complete_subset.male.value_counts(dropna=False).sort_index())


#### impute missing data
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

