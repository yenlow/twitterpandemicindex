# Reads in the places identified with geonameid
# Join with df_geonames to get geo metadata
# Term expansion with dict_synonymns to get lexical variants
# mostly lexical variants from lower().strip and then re.sub('\.','')
#
# Final output is a pd df df_place2geonameid (pickled and tsv) with columns:
# 'geonameid', 'asciiname', 'country code', 'hierarchy', 'feature code',
# 'latitude', 'longitude', 'admin1 code', 'admin2 code', 'population',
# 'synonyms'

import pickle
import pandas as pd
from place_norm.dict_places import df_geonames
from utils.utils import force_int_or_null

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

#output_suffix = '20200424_235625'
#loc_mappings = f'data/locations_mapping_{output_suffix}.tsv'
loc_mappings = 'data/locations_mapping.tsv'

out_tsv = 'data/df_place2geonameid.tsv'
out_pkl = 'data/df_place2geonameid.pkl'

df_mapped = pd.read_csv(loc_mappings, sep="\t")

# load dict_synonymns of case/punctuation variants
# mostly lexical variants from lower().strip and then re.sub('\.','')
dict_synonymns = pickle.load(open('../../data/place_norm/dict_synonymns.pkl', "rb"))
dict_synonymns['us']

# save the lexical variants into a column in df_mapped (list of lexical variants)
df_mapped['lexical_variants'] = (df_mapped.place_norm.apply(lambda x: dict_synonymns.get(x,'')))
# reformat column of list of lexical variants into a string
df_mapped['lexical_variants'] = df_mapped['lexical_variants'].apply(lambda x: ','.join([str(i) for i in x]))

# collect set of place_norm group by geonameid (i.e. roll up by geonameid)
# ditto for lexical variants
place_norm_set = (df_mapped.groupby('geonameid')['place_norm'].apply(lambda x: set(x)))
synonyms_set = (df_mapped.groupby('geonameid')['lexical_variants'].apply(lambda x: ','.join(x)))

# create the grouped by df with unique geonameid
df_mapped_set = pd.concat([place_norm_set,synonyms_set], join='outer', axis=1)
df_mapped_set.index   #geonameid

# join the mapped geonameid with df_geonames to get other geonames metadata
col_geonames_desired = ['geonameid','asciiname','country code',
                        'hierarchy','feature code','latitude','longitude',
                        'admin1 code', 'admin2 code','population','place']
df_geonames.index   #doesn't need to be geonameid
df_place2geonameid = df_mapped_set.merge(df_geonames[col_geonames_desired], on='geonameid', how='left')
df_place2geonameid.hierarchy = df_place2geonameid.hierarchy.apply(force_int_or_null).astype('float',copy=False) #use float to allow np.nan


# append place_norm set to places list
def fn(x):
    allStr = f"{x.lexical_variants},{x.place}"
    l = allStr.lower().split(',')
    s = {i for i in l if i!=''}
    return x.place_norm.union(s)

df_place2geonameid['synonyms'] = df_place2geonameid.apply(fn, axis=1)
df_place2geonameid.rename(columns={'place_norm':'place_queried'}, inplace=True)
df_place2geonameid.drop(columns=['lexical_variants','place'],inplace=True)

df_place2geonameid = df_place2geonameid[['asciiname','country code',
                                         'place_queried','geonameid',
                                        'hierarchy','feature code','latitude','longitude',
                                        'admin1 code', 'admin2 code','population','synonyms']]

# save df_place2geonameid
df_place2geonameid.to_pickle(out_pkl)
df_place2geonameid.to_csv(out_tsv, sep="\t", index=False)