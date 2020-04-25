# Reads in the places identified with geocoder api (mapquest)
# Join with df_geonames to get geo metadata
# Term expansion with dict_synonymns to get lexical variants
# mostly lexical variants from lower().strip and then re.sub('\.','')
#
# Final output is a pd df_api2geonameid (pickled and tsv) with columns:
# 'asciiname', 'country code'
# 'place_queried', 'geonameid',
# 'hierarchy', 'feature code', 'latitude', 'longitude',
# 'admin1 code', 'admin2 code', 'population','synonyms'

import pickle
import pandas as pd
from place_norm.dict_places import df_geonames

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

output_suffix = '20200424_235625'
loc_api = f'data/locations_api_{output_suffix}.tsv'

out_tsv = 'data/df_api2geonameid.tsv'
out_pkl = 'data/df_api2geonameid.pkl'

df_api = pd.read_csv(loc_api, sep="\t")

# load dict_synonymns of case/punctuation variants
# mostly lexical variants from lower().strip and then re.sub('\.','')
with open('data/dict_synonymns.pkl', 'rb') as f:
    dict_synonymns = pickle.load(f)
dict_synonymns['us']

# save the lexical variants into a column in df_api (list of lexical variants)
df_api['lexical_variants'] = (df_api.place_queried.apply(lambda x: dict_synonymns.get(x,'')))
# reformat column of list of lexical variants into a string
df_api['lexical_variants'] = df_api['lexical_variants'].apply(lambda x: ','.join([str(i) for i in x]))

df_api['place_norm'] = df_api['place_norm'].str.lower()
df_geonames['asciiname'] = df_geonames['asciiname'].str.lower()

# collect set of place_norm group by geonameid (i.e. roll up by geonameid)
# ditto for lexical variants
place_set = (df_api.groupby(['place_norm','country_code'])['place_queried'].apply(lambda x: set(x)))
synonyms_set = (df_api.groupby(['place_norm','country_code'])['lexical_variants'].apply(lambda x: ','.join(x)))

# create the grouped by df with unique geonameid
df_api_set = pd.concat([place_set,synonyms_set], join='outer', axis=1)
df_api_set.index.names = ['asciiname','country code']

# join the mapped geonameid with df_geonames to get other geonames metadata
col_geonames_desired = ['geonameid','asciiname','country code',
                        'hierarchy','feature code','latitude','longitude',
                        'admin1 code', 'admin2 code','population','place']

tmp = df_api_set.merge(df_geonames[col_geonames_desired],
                       on=['asciiname','country code'],
                       how='inner', right_index=True)

# window and sort by hierarchy asc and pop desc
tmp.hierarchy = tmp.hierarchy.astype('int')
tmp.population = -tmp.population.astype('float')  #reverse populaton rank
ranks = tmp.groupby('asciiname')[['hierarchy','population']].rank(method='min')
tmp.population = -tmp.population        #reverse populaton back
df_api2geonameid = tmp[(ranks.hierarchy==1) & (ranks.population==1)]


# append place_norm set to places list
def fn(x):
    allStr = f"{x.lexical_variants},{x.place}"
    l = allStr.lower().split(',')
    s = {i for i in l if i!=''}
    return x.place_queried.union(s)

df_api2geonameid['synonyms'] = df_api2geonameid.apply(fn, axis=1)

df_api2geonameid.drop(columns=['lexical_variants','place'],inplace=True)
df_api2geonameid.reset_index(inplace=True)

# save df_place2geonameid
df_api2geonameid.to_pickle(out_pkl)
df_api2geonameid.to_csv(out_tsv, sep="\t", index=False)
